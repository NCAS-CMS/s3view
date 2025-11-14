import cmd2
import logging
from pathlib import Path
from cfs3.s3core import get_client, get_locations, lswild, desanitise_metadata
from cfs3.skin import _i, _e, _p, _err, fmt_size, fmt_date, ColourFormatter
from minio.deleteobjects import DeleteObject
from minio.commonconfig import CopySource
from minio.tagging import Tags
from concurrent.futures import ThreadPoolExecutor, as_completed
import itertools
from io import StringIO
import argparse
from cfs3.drs_view import drs_view, drs_metaview, drs_select
import bitmath


logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)


def fetch_metadata(client, bucket, file_dict):
    """ Helper function to clean up calling metadata signature"""
    return file_dict, client.stat_object(bucket, file_dict['n'])


def match_metadata(client, bucket, object_name, matches):
    """ Helper function to grab only files with metadata matches"""
    result = client.stat_object(bucket, object_name)
    meta = {k[11:]: v for k, v in result.metadata.items() 
            if k.startswith('x-amz-meta')}
    meta = desanitise_metadata(meta)
    for k, v in matches.items():
        if k not in meta:
            return False, object_name
        else:
            if meta[k] != v:
                return False, object_name
    return True, object_name


def key_value(s: str):
    """ 
    Parse a key-=value string into a tuple (key, value).
    Raises argparse.ArgumentTypeError if format is invalid.
    """
    parts = s.split('=')
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"Expected key=value format, got '{s}'")
    key, value = parts
    key, value = key.strip(), value.strip()
    if not key or not value:
        raise argparse.ArgumentTypeError(
            f"Neither key nor value can be empty in '{s}'")
    return key, value


class OutputHandler:
    """ 
    Provides indirection through to cmd2 poutput, but in such a way
    that methods can keep their output for piping, and responses can
    be cached.

    This should be called in the init method of a cmd2 instance, something like

        self.output_handler=OutputHandler(self)
        self.houtput=self.output_handler.write

    which means you can use self.houtput(stuff for output) instead of 
    self.poutput (the default cmd2 option).

    A future version will include persistence, and cache maintenance.
    """
    def __init__(self, cmd_instance):
        """ 
        Instantiate in the init method of a cmd2 instance 
        """
        self.cmd = cmd_instance
        self.cache = {}
        self.lines = []
        self.signature = None
        self.last_cache = None

    def write(self, string):
        """ 
        this is the output method, writes normal output
        and grabs a copy for later use in pipe and/or to
        go to cache 
        """
        # send straight to output
        self.cmd.poutput(string)
        for line in string.splitlines():
            self.lines.append(line)
            self.cmd.log.debug(f'[cache] received {line}')

    def start(self):
        self.lines = []
        self.signature = None

    @staticmethod
    def __make_signature(method_name, arg_namespace):
        sig_items = []
        for k, v in vars(arg_namespace).items():
            # skip cmd2 internal wrapper objects
            if k.startswith("cmd2_"):
                continue
            # convert lists or other mutable types to tuples
            if isinstance(v, list):
                v = tuple(v)
            sig_items.append((k, v))
        # sort for deterministic ordering
        sig_items.sort()
        return (method_name, tuple(sig_items))

    def start_method(self, method_name, arg_namespace):
        """
        Call this with at the beginning of a method.
        If this returns anything but None, you have got
        something in the cache, and you can decide what
        you want to do with it.
        """
        signature = self.__make_signature(method_name, arg_namespace)
        self.signature = signature
        if signature in self.cache:
            self.cmd.log.debug('[cache] start')
            self.last_cache = self.cache[signature]
            return self.last_cache
        else:
            self.lines = []
            return None

    def end_method_and_cache(self):
        """ 
        Call this at the end of a method to populate the cache
        """
        if self.signature:
            self.cache[self.signature] = self.lines
            self.last_cache = self.lines
            self.cmd.log.debug(
                f'[cache] population had {len(self.lines)} lines')
        else:
            self.cmd.log.debug('[cache] not used')
            self.signature = None


class s3cmd(cmd2.Cmd):
    """ 
    s3cmd is the class which implements the s3view capabilities for viewing contents in S3 repositories

    The methods of this class implement the commands seen in the ``s3view`` environment. 
    There is some limited capability to "pipe" information between selected commands 
    (using :: for piping), and the output of all commands can be piped to the normal 
    unix shell.

    """

    _autodoc_attrs = ['pipe_producers', 'pipe_consumers']
    pipe_producers = ['ls',]
    """ List of commands that can produce content to consume via internal pipe "::" """
    pipe_consumers = ['p5dump',]
    """ List of commands that can consume content from an internal pipe "::" """
    allow_redirection = True

    def __init__(self, path=None, config_file=None):
        """
        Initialise Command Line Environment 
        Args:
            path (_type_, optional): This is the initial location from the selection in your config file. Defaults to None.
            config_file (_type_, optional): This is the location of your S3 configuration(s).
            Defaults to None (in which case s3view will attempt to find and use ~/.mc/config.json)
        """

        # Set include_ipy to True to enable the "ipy" command which runs an interactive IPython shell
        super().__init__(include_ipy=True)

        self.log = logging.getLogger('cfs3iew')

        #controls level (need to set this to get the logger to process anything, 
        #if this is below the console level, we get nothing).
        self.log.setLevel(logging.INFO)

        self.console = logging.StreamHandler()
        # if the log level lets it through, this controls the actual output to console
        self.console.setLevel(logging.INFO)
        self.console.setFormatter(ColourFormatter('%(levelname)s: %(message)s'))
        self.log.addHandler(self.console)

        self.poutput(_i('You have entered a lightweight management tool for organising "files" inside an S3 object store'))
        self.prompt = 's3> '
        self.debug = False
        self.alias, self.bucket, self.path = None, None, None
        self.locations = " ".join(get_locations(config_file))
        self.buckets = []
        self.config = config_file

        if path is None:
            self.prompt = 's3> '
            self._noloc()
        else:
            self._navconfig(path)

        self.starting = True
        self.mydirs = None
        self.maybe_anon = False
       

        self.hidden_commands = {'eof', '_relative_run_script',
                                'alias', 'macro', 'edit', 'run_pyscript', 
                                'run_script', 'shortcuts', 'shell', 'py', 
                                'history', 'set', 'suspend'
                                }

        self.output_handler = OutputHandler(self)
        self.houtput = self.output_handler.write

    def get_names(self):
        # This method returns a list of all command method names
        names = super().get_names()
        filtered = []
        for name in names:
            if name.startswith("do_"):
                cmd_name = name[3:]
                if cmd_name in self.hidden_commands:
                    continue
            filtered.append(name)
        return filtered
       
    def _noloc(self):
     
        self.poutput(_i('Your available minio locations are: ')+self.locations)
        self.poutput(_i('Choose one with "loc x" '))

    def _confirm(self, message, default='N'):
        self.poutput(f'{message} [y/n]')
        while True:
            ans = input()
            if ans == "":
                ans = default 
            try:
                return {'y': True, 'n': False}[ans.lower()]
            except Exception:
                self.poutput(_err('Please respond with \'y\' or \'n\'.\n'))

    def _navconfig(self, target):
        """ Unpick a navigation command and configure """
        bits = target.split('/')
        self.log.debug(f'[navconfig] is seeing {bits}')
        if bits[0] != self.alias:
            try:
                self.client = get_client(bits[0], config_file=self.config)
            except ValueError as e:
                self.poutput(_err(e))
                return
            self.alias = bits[0]
            self.prompt = _p(f'{self.alias}> ')

            try:
                existing_buckets = self.buckets
                buckets = self.client.list_buckets()
                if buckets is not None:
                    self.buckets = [b.name for b in buckets]
                else:
                    self.buckets = []
            except Exception as e:
                if 'Access Denied' in str(e):
                    self.poutput(_err(f'Access Denied to {self.alias}, you may still be able to cb into anonymous buckets'))
                    self.maybe_anon = True
                    self.bucket = None
                    self.path = None
                    return 
                else:
                    self.poutput(_err(e))
                    self.buckets=existing_buckets
            self.log.debug(f'[navconfig] buckets set to {self.buckets}')

        match len(bits):
            case 1: 
                self.bucket = None
                self.path = None
            case 2:
                self.bucket = bits[1]
                self.path = None
            case 3:
                self.bucket = bits[1]
                self.path = bits[2]
    
    def _recurse(self, path, match=None, limit=None):
        """ 
        From a given path, head down the tree and do some summing.
        We can constrain ourself to a set of matching objects.
        We can also constain how many objects we want to look at.
        """
        if path == "":
            prefix = None
        else:
            prefix = path
        # this is a generator
        objects = self.client.list_objects(self.bucket,
                                           include_user_meta=True,
                                           prefix=prefix)

        # if we are limited, we can collapse our generator to a list
        if limit is not None:
            objects = list(itertools.islice(objects, limit))
          
        if match is not None:
            objects = [o for o in objects if Path(o.object_name).match(match)]

        sum = 0
        files = 0
        dirs = 1
        mydirs = []
        myfiles = []
        for o in objects:
            if o.is_dir:
                path = f'{path}/{o.object_name}'
                dsum, dfiles, ddirs, md, mf = self._recurse(path)
                sum += dsum
                files += dfiles
                dirs += 1
                mydirs.append([o.object_name, fmt_size(dsum)])
            else:
                sum += o.size
                files += 1
                myfiles.append({'n': o.object_name, 
                                's': fmt_size(o.size),
                                'd': fmt_date(o.last_modified),
                                't': o.tags,
                                })
        return sum, files, dirs, mydirs, myfiles

    def _cd_lander(self, path):
        """
        This internal routine reports information about a particular path
        """
        self.path = path
        volume, nfiles, ndirs, mydirs, myfiles = self._recurse(path)
        if path == '':
            path = '/'
        self.poutput(_i('Location: ') + path + _i(' contains ') +
                     fmt_size(volume) + _i(' in ') + str(nfiles) +
                     _i(' files/objects.'))
        self.poutput(_i('This directory contains ') + str(len(myfiles)) + 
                     _i(' files and ') + str(len(mydirs)) + 
                     _i(' directories.'))
        if len(mydirs) > 0:
            self.poutput(_i('Sub-directories are : ') +
                         _e(' '.join([f'{d[0]}({d[1]})' for d in mydirs])))
        self.mydirs = mydirs

    def __handle_path(self, path):
        """
        Generate the appropriate path prefix from the current
        path and where the user is trying to go
        """
        if self.path is None:
            self.path = ""
        if path == "":
            return self.path
        elif path is None:
            return self.path
        elif path == '..':
            if self.path == "":
                return self.path
            else:
                p = str(Path(self.path).parent)
                return {True: '', False: p}[p == '.']
        else:
            return self.path + path
        
    def _getmetadata(self, myfiles):
        mymetadata = []
        # loop runs with minimum of 32 or the number of processors multiplied by 5, based on Python’s default configuration.
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(
                       fetch_metadata, self.client, self.bucket, f):
                       f for f in myfiles}
            for future in as_completed(futures):
                try:
                    f, result = future.result()
                    meta = {k[11:]: v for k, v in result.metadata.items()
                            if k.startswith('x-amz-meta')}
                    mymetadata.append((f, desanitise_metadata(meta)))
                except Exception as e:
                    self.poutput(_err(f'Error fetching metadata {e}'))
        mymetadata = sorted(mymetadata, key=lambda x: x[0]['n'])
        return mymetadata

    def precmd(self, statement):
        """
        Handles internal piping using '::'. Splits the line into LHS and RHS.
        Executes LHS first (output is cached), then returns RHS to be executed next.
        """
        line = statement.raw  # get raw input
        self.log.debug(f'[precmd] received: {repr(line)}')
        self.log.debug(f'[precmd] buckets are {self.buckets}')

        if '::' in line:
            lhs, rhs = map(str.strip, line.split('::', 1))
            try:
                lhs_cmd = lhs.split()[0]
                rhs_cmd = rhs.split()[0]

                # Ensure only allowed commands participate
                if lhs_cmd not in self.pipe_producers:
                    self.poutput(_err(f"{lhs_cmd} cannot produce output for another internal command"))
                    return ''

                if rhs_cmd not in self.pipe_consumers:
                    self.poutput(_err(f"{rhs_cmd} does not know how to consume previous output"))
                    return ''

                # Execute LHS; output should be both cached and suppressed
                original_stdout = self.stdout        # Save current stdout
                buffer = StringIO()
                self.stdout = buffer
                lhserror = False
                try:
                    self.onecmd_plus_hooks(lhs)
                finally:
                    self.stdout = original_stdout
                    for line in buffer.getvalue().splitlines():
                        if line.startswith('EXCEPTION'):
                            self.output(line)
                            lhserror = True
                if lhserror:
                    self.__pipe_input = None
                    self.poutput(_err('Did not proceed to right hand side of pipe'))
                    return ''

                # Store the last cached output for RHS
                self.__pipe_input = self.output_handler.last_cache
                self.log.debug(f'[precmd] stored [[{self.__pipe_input}]]')

                self.log.debug(f"[precmd] Going to {rhs_cmd} with piped input")
                return rhs  # RHS will now be dispatched by cmd2

            except Exception as e:
                self.poutput(_err(str(e)))
                self.__pipe_input = None
                return ''
        else:
            # No pipe, clear any stale input
            self.__pipe_input = None
            return line

    def cached_columnize(self, *args, **kwargs):
        """ intercept columnize and make sure we get output to cache """
        self.log.debug('intercepting columnize')
        buf = StringIO()
        old_stdout = self.stdout
        self.stdout = buf
        self.columnize(*args,**kwargs)
        self.stdout = old_stdout
        tocache = buf.getvalue().splitlines()
        self.log.debug(f'[caccol] found {len(tocache)} lines to cache')
        for line in tocache:
            self.houtput(line)

    def do_lb(self,arg=None):
        """ 
        List buckets in the current location.

        Information about bucket contents can only be gathered after using the ``cb`` command.
        """
        self.log.debug(f'[do_lb] Attempting to list buckets for {arg}')
        if arg is not None:
            if arg == "":
                pass
            elif arg != self.alias:
                self.log.debug(f'[do_lb] renavigating for {arg} given alias {self.aliase}')
                self._navconfig(arg)
        self.poutput(_i('Buckets:  ')+' '.join(self.buckets))

    loc_args = cmd2.Cmd2ArgumentParser()
    loc_args.add_argument('alias', help='Where alias is a valid alias from your minio config file')
    @cmd2.with_argparser(loc_args)
    def do_loc(self, arg):
        """
        Set context to a particular S3 location as described in user minio config file.

        (We use the minio config file to allow users to keep credentials for multiple S3
        locations in one place.)
        """
        try:
            self._navconfig(arg.alias)
            if not self.maybe_anon:
                self.do_lb()
        except ValueError:
            self.poutput(_err(f'Location {arg.alias} not in your minio config file '))
            self._noloc()

    cb_args = cmd2.Cmd2ArgumentParser()
    cb_args.add_argument('bucket', help='Bucket should be a valid bucket in your current location')
    @cmd2.with_argparser(cb_args)
    def do_cb(self, arg):
        """
        Change to a (new) bucket. 
        
        This is not treated as just a move to another directory, as the notion of a bucket is quite
        different in S3 from that of a directory. It's more like moving to the root of a different
        file system.
        """
        bucket = arg.bucket
        if not self.maybe_anon and bucket not in self.buckets:
            self.poutput(_err(f'Bucket [{bucket}] does not exist'))
        else:    
            self.bucket = bucket
            volume, nfiles, ndirs, mydirs, myfiles = self._recurse('')
            self.poutput(_i('Bucket: ') + bucket + _i(' contains ')+ fmt_size(volume) + _i(' in ') + str(nfiles) + _i(' files/objects.'))
        self.path=""
    
    ls_args = cmd2.Cmd2ArgumentParser()
    ls_args.add_argument('-l', '--long', action='store_true', help='Same as -s -d -m')
    ls_args.add_argument('-s', '--size', action='store_true',help="Tell us about size")
    ls_args.add_argument('-w', '--width', nargs='?', default=90, type=int, help='width of display for standard output')
    ls_args.add_argument('-m', '--metadata', action='store_true',help="Show user metadata")
    ls_args.add_argument('-t', '--tags', action='store_true',help="Show tags")
    ls_args.add_argument('-d', '--date', action='store_true',help="Show dates")
    ls_args.add_argument('-o', '--order', nargs='?', help="Order by size|date")
    ls_args.add_argument('-n', '--max_number', type=int, help='Limit the number of files returned')
    ls_args.add_argument('path', nargs='?',help='Path should be a valid path in your current bucket and location, possibly with a wildcard.')
 
    @cmd2.with_argparser(ls_args)
    def do_ls(self, arg='/'):
        """ 
        List the files and directories in a bucket, potentially with a wild card. 

        """

        def reorder(mymeta):
            """ More interesting order of user metadta """
            copied = [k for k in mymeta]
            priority_order = ['standard-name','long-name','domain','shape']
            result = {}
            for p in priority_order:
                if p in copied:
                    result[p]=mymeta[p]
                    copied.remove(p)
            for p in copied:
                result[p]=mymeta[p]
            return result


        if self.path is None: 
            self.path = '/'

        if arg.max_number is not None:
            limit = arg.max_number
        else:
            limit = None
        
        extras = arg.path
        if arg.order not in [None, 'size', 'date']:
            print(arg.order)
            self.poutput(_err('Unrecognised order option'))


        cache_available = self.output_handler.start_method('do_ls', arg)
        if cache_available:
            self.poutput(_i('Using cached information'))
            #FIXME make that optional, use times etc
            for line in cache_available:
                self.poutput(line)
            return
            
            
        volume, nfiles, ndirs, mydirs, myfiles = self._recurse(self.path, extras, limit=limit)
        if limit is None:
            self.houtput(_i('Location: ') + self.path + _i(' contains ')+ fmt_size(volume) + _i(' in ') + str(nfiles) + _i(' files/objects.'))
            directory = 'directory'
            if extras: 
                directory = "match"
            self.houtput(_i(f'This {directory} contains ')+ str(len(myfiles)) + _i(' files and ') + str(len(mydirs)) + _i(' directories.'))
        else:
            self.houtput(_i(f'Listing {max(nfiles,limit)} files/objects ('+fmt_size(volume)+')'))
            

        width = arg.width

        if arg.long or arg.metadata or arg.size or arg.date or arg.tags:
            mlen = 0
            for f in myfiles:
                lf = len(f['n'])
                if lf > mlen:
                    mlen = lf
            
            if arg.long or arg.metadata:
                mymetadata = self._getmetadata(myfiles)
            else:
                mymetadata = [(f,None) for f in myfiles]

            strings = []
            for f,meta in mymetadata:
                string = f"{Path(f['n']).name:<{mlen}}  "
                if arg.long or arg.size:
                    string += _e(f"{f['s']:>10}")
                if arg.long or arg.date:
                    string += f"   {f['d']}"
                if arg.tags:
                    string += f"   {f['t']}"
                if arg.long or arg.metadata:
                    if arg.long:
                        pretty_meta ='  {'
                    else:
                        pretty_meta = '\n   {'
                    meta = reorder(meta)
                    for k,v in meta.items():
                        pretty_meta += _e(k)+f': {v}, '
                    string += pretty_meta[:-2]+'}'
                    if not arg.long:
                        string +='\n'
                strings.append({'s':string,'d':[f['d']],'v':f['s']})
            
            match arg.order:
                case None:
                    pass
                case 'size':
                    strings = sorted(strings, key=lambda x: bitmath.parse_string(x['v']))
                case 'date':
                    strings = sorted(strings, key=lambda x: x['d'])
            for s in strings:
                self.houtput(s['s'])
                    
        else:
            self.log.debug('[ls] b4 columnize')
            self.cached_columnize([f"{Path(f['n']).name}" for f in myfiles],display_width=width)
            self.log.debug('[ls] after columnize')

        if len(mydirs) > 0: 
            if len(mydirs) > 3:
                self.houtput(_i('Sub-directories are : '))
                self.cached_columnize([_e(f'{d[0]}({d[1]})') for d in mydirs],display_width=width)
            else:
                self.houtput(_i('Sub-directories are : ')+_e('  '.join([f'{d[0]}({d[1]})' for d in mydirs])))

        self.output_handler.end_method_and_cache()

    fi_args = cmd2.Cmd2ArgumentParser()
    fi_args.add_argument('-p', '--path', default=None, help='path prefix in which you want to find the metadata matches')
    fi_args.add_argument('-w', '--width', nargs='?', default=90, type=int, help='width of display for standard output')
    fi_args.add_argument('keyvals',nargs='*', help="Metadata key-value pairs in the format key=value which you want to match")
    @cmd2.with_argparser(fi_args)
    def do_match(self, args):
        """
        Find files which match a set of metadata expressed as key value pairs, optionally matching a
        particular path. 

        This metadata match is using the DRS metadaata which has been uploaded with the file.
        """ 
        if self.bucket is None:
            self.poutput(_err('Must select bucket'))
            return
        path = args.path
        pairs = {}
        for kv in args.keyvals:
            if "=" in kv:
                k,v = kv.split('=',1)
                pairs[k]=v
            else:
                self.poutput(_err('Invalid key pair: ')+kv)
                return
        objects = self.client.list_objects(self.bucket, prefix=self.path)
        if path is not None:
            objects = [o for o in objects if Path(o.object_name).match(path)]

        matches = []
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(match_metadata, self.client, self.bucket, o.object_name, pairs): o for o in objects}
            for future in as_completed(futures):
                try:
                    status, name = future.result()
                    if status:
                        matches.append(name)
                except Exception as e:
                    self.poutput(_err(f'Error fetching metadata for {name} {e}'))
        if matches == []:
            self.poutput(_e('No matches'))
        else:
            self.columnize(matches,display_width=args.width)

    
    cd_args = cmd2.Cmd2ArgumentParser()
    cd_args.add_argument('path', nargs='?',help='Path should be a valid path in your current bucket and location.')
    @cmd2.with_argparser(cd_args)
    def do_cd(self,line):
        """
        Change default position in bucket to expose contents as if it were a file system directory.

        This exploits the minio notion of prefixes to give a directory like view of some of the
        content in a bucket.
        """
        if self.bucket is None:
            self.poutput(_err('You need to select a bucket first ("cd bucket_name")'))
            return
        path = line.path
        self.path = self.__handle_path(path)
        return self._cd_lander(self.path)
    
    def complete_cd(self, text, line, start_index, end_index):
        """ Used for tab completing directories"""
        prefix = self.__handle_path(text)
        mydirs = [o.object_name for o in self.client.list_objects(self.bucket,prefix=prefix) if o.is_dir]
        if text:
            return [
                adir for adir in mydirs
                if adir.startswith(text)
            ]
        else:
            return mydirs

    mb_args = cmd2.Cmd2ArgumentParser()
    mb_args.add_argument('bucket',help='The name of a new bucket to create in your current location')
    @cmd2.with_argparser(mb_args)
    def do_mb(self, arg):
        """ 
        Make bucket, return error if existing.
        """
        bucket_name = arg.bucket
        # update the list
        self.buckets = [b.name for b in self.client.list_buckets()]
        
        if bucket_name in self.buckets:
            self.poutput(_err(f'Bucket {bucket_name} already exits')) 
            self.cb(None)
        r = self.client.make_bucket(bucket_name)
        if r:
            self.poutput(self._err('Unable to make bucket properly'))
            return 
        self.buckets.append(bucket_name)
        return self.do_cb(f'cb {bucket_name}')
    

    rm_args = cmd2.Cmd2ArgumentParser()
    rm_args.add_argument('targets',nargs='+',help='filenames and/or paths to be removed')
    @cmd2.with_argparser(rm_args)
    def do_rm(self, arg):
        """ 
        Remove a list of objects, including those which may be generated from wild card matches. 
        """
        if self.bucket is None:
            self.poutput(_err("set bucket first"))
            return
        objects = []
        for a in arg.targets:
            objects = self.client.list_objects(self.bucket, prefix=self.path)
            objects = [o.object_name for o in objects if Path(o.object_name).match(a)]
        # in principle, with wild cards, we could get duplicates
        objects = list(set(objects))
        if len(objects) > 0:
            self.poutput(_i('\nList of objects for deletion:'))
            self.poutput(_e(" ".join(objects)))
            if self._confirm(_p('Delete these files from {bucket}?')):
            
                delete_list = [DeleteObject(o) for o in objects]
                # this would be lazy if I didn't force it with the list and error parsing
                errors = list(self.client.remove_objects(self.bucket, delete_list))
                if errors != []:
                    for error in errors:
                        self.poutput(_err(f"error occurred when deleting object {error}"))
                    lf = len(objects)
                    le = len(errors)
                    self.poutput(_p(f"{lf-le}/{lf} files deleted from {self.bucket} in {self.alias}"))
                    self.poutput(_p('You will need to check which files were actually deleted'))
                else:
                    self.poutput(_i(f"{len(objects)} objects deleted from {self.bucket} in {self.alias}"))
        else:
            self.poutput(_i('Nothing to remove'))

    mv_args = cmd2.Cmd2ArgumentParser()
    mv_args.add_argument('targets',nargs=2,help='filenames and/or paths to be removed, e.g. mv fileA fileB')
    @cmd2.with_argparser(mv_args)
    def do_mv(self, command):
        """
        Rename files within a bucket (server side).

        This is an expensive operation as it really involves a server-side copy, not a renaming
        operation as you might expect in a normal file system.
        """
        try:
            source, target = tuple(command.targets)
            self.poutput(_i('Command is mv ')+ source+ _i(' to ')+target)
        except Exception:
            self.poutput(_err(f'Invalid mv command - mv "{command.targets}"'))
            return self.do_cd(self.path)
        
        sfiles = lswild(self.client, self.bucket, source, objects=True)
        ncopies = len(sfiles)
        if ncopies == 0:
            self.poutput(_i('No files match {source}'))
            return self.do_cd()
        elif ncopies == 1:
            if target.endswith('/'):
                targets = ['f{target}/{sfiles[0].object_name}']
            else:
                targets = [target] 
        elif ncopies > 1:
            if not target.endswith('/'):
                self.putput(_err(f'Need a directory target to mv {len(sfiles)} files -  target must end with a /'))
                return self.do_cd()
            targets = [f'{target}{o.object_name}' for o in sfiles]
        volume = fmt_size(sum([o.size for o in sfiles]))

        self.poutput(_i('\nList of movements:'))
        for o,t in zip(sfiles,targets):
            self.poutput(_e(f'mv {o.object_name} to {t}'))
        self.poutput(_p('This move is done as a server side copy - it is not "just" a rename!'))
        if self._confirm(_p(f'Move these files ({volume}) ?')):
            for o,t in zip(sfiles, targets):
                src = CopySource(self.bucket, o.object_name)
                result = self.client.copy_object(self.bucket, t, src)
                if o.etag != result.etag:
                    self.poutput(_err(f'Failed copy of {o.object_name} - mv operation terminated'))
                    return self.cd(self.path)
                self.poutput(f'Created {_e(result.object_name)}')
                self.client.remove_object(self.bucket,o.object_name)

        return self.do_cd(self.path)

    tag_args = cmd2.Cmd2ArgumentParser()
    tag_args.add_argument('path', nargs=1,help='Path should be a valid object match (i.e. an object path, possibly with a wildcard).')
    tag_args.add_argument('value',nargs=1, help='Value for tag')
    tag_args.add_argument('key', nargs=1,help='Key for a tag')
    @cmd2.with_argparser(tag_args)
    def do_tag(self, targets):
        """
        Tag a previously existing object with a key value pair.

        Allows users with object stores who support tagging to tag objects dynamically
        rather than utilise the user metadata option. Many object stores, including
        the author's one, do not support this. This might work for you, it doesn't
        work for me. Let me know if it does.
        """
        if self.bucket is None:
            self.poutput(_err('Need to set bucket before tagging anything'))
            return 
        prefix = targets.path
        key = targets.key[0]
        value = targets.value[0]
        objects = self.client.list_objects(self.bucket,prefix=prefix)
        for o in objects:
            if o.is_dir:
                continue
            tags = o.tags
            if tags is None:
                tags=Tags()
            tags[key]=value
            try:
                self.client.set_object_tags(self.bucket, o.object_name, tags)
            except Exception as e:
                self.poutput(_err(e))
                self.poutput(_err('Unable to tag object(s), your object store implementation may not support this'))
                return

    tag1_args = cmd2.Cmd2ArgumentParser()
    tag1_args.add_argument('path', nargs=1,help='Path should be a valid object match (i.e. an object path, possibly with a wildcard).')
    def do_pwd(self,arg):
        """
        Do the equivalent of a print working directory, that is, show whwere you are in the 
        current bucket. Here the root (/) will be the root of a particular bucket. We do
        not want to encourage folks to think of different buckets as being part of the same
        file system.
        """
        if self.alias is None:
            self._noloc()
        else:
            if self.bucket:
                if self.path == "":
                    path = '/'
                else:
                    path = self.path
                self.poutput(_i('Current working directory ') + path + _i(' in bucket ') + self.bucket)
            else:
                self.do_lb()

    

    drs_default = 'Variable,Source,Experiment,Variant,Frequency,Period,nFields'
    drs_args = cmd2.Cmd2ArgumentParser()
    drs_args.add_argument('path', nargs='?',
                            help='Path should be a valid path in your current bucket and location, possibly with a wildcard.')
    drs_args.add_argument('drs', nargs='?', 
                            default=drs_default,
                            help=('Comma seperated string of DRS components to be used for contents '
                            '(ignored for metadata option)\n'
                            f'default = {drs_default}'))
    drs_args.add_argument('-c','--collapse_list',default='',
                            help='Comma seperated string of DRS terms where short lists are wanted')
    drs_args.add_argument('-u','--use_metadata',action='store_true',help="build drs-like view from metadata")
    drs_args.add_argument('-s','--select', type=key_value, action='append',
                            help='Specfiy DRS component selections as key=value (multipe -s allowed) and return listing')
    drs_args.add_argument('-o','--output',default='drs',nargs=1,help='Default output is a DRS view, alternative is "list" view.')
    @cmd2.with_argparser(drs_args)
    def do_drsview(self,arg):
        """ Extract DRS components at location """  

        if self.bucket is None:
            self.poutput(_err('You need to select a bucket first ("cd bucket_name")'))
            return

        if self.path is None:
            self.path = '/'

        extras = arg.path
        volume, nfiles, ndirs, mydirs, myfiles = self._recurse(self.path, extras)

        selects = dict(arg.select) if arg.select else {}

        if selects:
            myfiles, skipped = drs_select(myfiles, selects, arg.drs)

        if arg.output:
            output_arg = arg.output[0]
        else:
            output_arg='drs'

        if output_arg == 'list':
            if not arg.select:
                self.poutput(_e('List output only available with selection'))
                return
            for f in myfiles:
                self.poutput(f['n'])
            if skipped: 
                self.poutput(_e('Skipped the following files (no DRS match):'))
                for f in skipped:
                    self.poutput(f)
            return
        elif output_arg != 'drs':
            self.poutput(_e(f'output options are "drs" or "list" (you said "{arg.output}")'))

         
        #FIXME: move metadata select out of here, it doesn't work with drs_select for now.
        if arg.use_metadata:
            mymetadata = self._getmetadata(myfiles)
            output = drs_metaview(mymetadata, selects=selects, collapse=arg.collapse_list)
        else:
            myfiles = [f['n'] for f in myfiles]
            output = drs_view(myfiles, arg.drs, selects=selects, collapse=arg.collapse_list)

        for line in output:
            self.houtput(line)
            
        
    cfd_args = cmd2.Cmd2ArgumentParser()
    cfd_args.add_argument('object', nargs=1,help='object should be a valid object in your current bucket and location.')
    cfd_args.add_argument('-c','--complete',action='store_true', help='Display complete descriptions of cf fields')
    cfd_args.add_argument('-s','--short',action='store_true', help='Display short descriptions of cf fields')
    @cmd2.with_argparser(cfd_args)
    def do_cflist(self, arg):
        """ cflist a remote object """
        from cfs3.s3sci import cfread
        if self.bucket is None:
            self.poutput(_err('You need to select a bucket first ("cd bucket_name")'))
            return
        
        if '*' in arg.object[0]:
            if self.path is None: 
                self.path = '/'
        
            extras = arg.object[0]
            volume, nfiles, ndirs, mydirs, myfiles = self._recurse(self.path, extras)
            input_files = [f['n'] for f in myfiles]
            self.poutput(_i(f'Detailed listing for {len(myfiles)} files may be slow, consider using -m option instead (if possible).'))
        else:
            input_files = [arg.object[0],]

        for input_file in input_files:
            flist, output = cfread(self.alias,self.bucket, self.path, input_file, 
                                short= arg.short, complete=arg.complete)
            for o in output:
                self.poutput(o)

    def complete_cflist(self, text, line, start_index, end_index):
        """ Used for tab completing cfdump """
        
        #handle misuse of tab completion gracefully
        if text =='*':
            raise ValueError('Cannot tab complete wildcards')
        
        prefix = self.__handle_path(text)
        myobjs = [o.object_name for o in self.client.list_objects(self.bucket,prefix=prefix) if not o.is_dir]
        if text:
            return [
                adir for adir in myobjs
                if adir.startswith(text)
            ]
        else:
            return myobjs

    p5d_args = cmd2.Cmd2ArgumentParser()
    p5d_args.add_argument('object', nargs='?',help='object should be a valid HDF5 or NC4 file in your current bucket and location.')
    p5d_args.add_argument('-s','--special',action='store_true', help='Display special attributes of datasets in files (NOT IMPLEMENTED)')
    @cmd2.with_argparser(p5d_args)
    def do_p5dump(self, arg):
        """ 
        Use pyfive to approximate a ncdump -h on a remote object 
         Accepts:
            - normal filename argument
            - piped filenames from previous command via self.__pipe_input.
        """
        from cfs3.p5inspect import p5view

        if self.bucket is None:
            self.poutput(_err('You need to select a bucket first ("cd bucket_name")'))
            return
        
        input_files=[]

        if self.__pipe_input:
            self.log.debug('[p5dump] is piped')
            # Piped input can be a string or list of strings
            for line in self.__pipe_input[1:]:
                if line.split(' ')[0] != '':
                    input_files.append(line.strip())
                    
        elif arg.object:
            self.log.debug('[p5dump] is normal')
            input_files = [arg.object,]

        if not input_files:
            self.poutput(_err("No filename provided"))
            return

        for input_file in input_files:
            self.log.debug(f'[p5dump] using file [{input_file}]')
            output = p5view(self.alias, self.bucket, self.path, input_file, 
                                special = arg.special)
            for o in output:
                self.poutput(o)

     
    def complete_p5dump(self, text, line, start_index, end_index):
        """ Used for tab completing p5dump """
        
        #handle misuse of tab completion gracefully
        if text =='*':
            raise ValueError('Cannot tab complete wildcards')
        
        self.log.debug('[complete_p5dump] Completion handler active')
        prefix = self.__handle_path(text)
        myobjs = [o.object_name for o in self.client.list_objects(self.bucket,prefix=prefix) if not o.is_dir]
        if text:
            return [
                adir for adir in myobjs
                if adir.startswith(text)
            ]
        else:
            return myobjs

    def do_loglevel(self, arg):
        """
        Change logging level. Usage: loglevel [debug|info|warning|error|critical]
        """
        level = arg.strip().lower()

        levels = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
            'critical': logging.CRITICAL,
        }

        if level not in levels:
            self.poutput(_err("Usage: loglevel [debug|info|warning|error|critical]"))
            return

        self.log.setLevel(levels[level])
        self.console.setLevel(levels[level])
        self.poutput(f"Logging level set to {level.upper()}")



    def default(self,arg):
        if not self.starting:
            self.poutput(_err(f'Command not recognised (at alias={self.alias}, bucket={self.bucket}, path={self.path})'))
        self.starting=False
        self.log.debug('Default command triggered')
        self.do_lb()