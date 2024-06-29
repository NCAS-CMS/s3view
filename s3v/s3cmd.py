import cmd2
from pathlib import Path
from s3v.s3core import get_client, get_locations, lswild
from s3v.skin import _i, _e, _p, _err, fmt_size, fmt_date
from minio.deleteobjects import DeleteObject
from minio.commonconfig import CopySource
from minio.tagging import Tags
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_metadata(client, bucket, file_dict):
    """ Helper function to clean up calling metadata signature"""
    return file_dict, client.stat_object(bucket, file_dict['n'])

class s3cmd(cmd2.Cmd):
    """ 
    Provides a view into one or more S3 repositories
    configured with a user's minio mc environment.
   
    """
    def __init__(self, path=None):

        # Set include_ipy to True to enable the "ipy" command which runs an interactive IPython shell
        super().__init__(include_ipy=True)
        
        self.poutput(_i('You have entered a lightweight management tool for organising "files" inside an S3 object store'))
        self.prompt = 's3> '
        self.debug = True
        self.alias, self.bucket, self.path = None, None, None
        if path is None:
            self.prompt = 's3> '
            self._noloc()
            self.buckets = None
        else:
            self._navconfig(path)
        self.starting = True
        self.mydirs = None
        

    def _noloc(self):
        locations = " ".join(get_locations())
        self.poutput(_i('Your available minio locations are :')+locations)
        self.poutput(_i('Choose one with "loc x" '))

    def _confirm(self, message, default='N'):
        defstr = {"Y":'Y/n',"N":'n/Y'}[default]
        self.poutput(f'{message} [y/n]')
        while True:
            ans = input()
            if ans == "":
                ans = default 
            try:
                return {'y':True,'n':False}[ans.lower()]
            except:
                self.poutput(_err('Please respond with \'y\' or \'n\'.\n'))

    def _navconfig(self, target):
        """ Unpick a navigation command and configure """
        bits = target.split('/')
        if bits[0]!=self.alias:
            self.alias = bits[0]
            self.prompt = _p(f'{self.alias}> ')
            self.client = get_client(self.alias)
            self.buckets = [b.name for b in self.client.list_buckets()]
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

    def _recurse(self, path, match=None):
        """ 
        From a given path, head down the tree and do some summing
        """
        if path == "":
            prefix = None
        else:
            prefix = path
        objects = self.client.list_objects(self.bucket,include_user_meta=True, prefix=prefix)
        if match is not None:
            objects = [o for o in objects if Path(o.object_name).match(match)]

        sum  = 0
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
                files +=1
                myfiles.append({'n':o.object_name, 
                                's':fmt_size(o.size),
                                'd':fmt_date(o.last_modified),
                                't':o.tags,
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
        self.poutput(_i('Location: ') + path + _i(' contains ')+ fmt_size(volume) + _i(' in ') + str(nfiles) + _i(' files/objects.'))
        self.poutput(_i('This directory contains ')+ str(len(myfiles)) + _i(' files and ') + str(len(mydirs)) + _i(' directories.'))
        if len(mydirs) > 0:
            self.poutput(_i('Sub-directories are : ')+_e(' '.join([f'{d[0]}({d[1]})' for d in mydirs])))
        self.mydirs=mydirs

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
                return {True:'',False:p}[p=='.']
        else:
            return self.path + path

    def do_lb(self,arg=None):
        """ Navigate around a S3 service"""
        if arg is not None:
            if arg == "":
                pass
            elif arg != self.alias:
                print('doing')
                self._navconfig(arg)
        self.poutput(_i('Buckets:  ')+' '.join(self.buckets))

    loc_args = cmd2.Cmd2ArgumentParser()
    loc_args.add_argument('alias', help='Where alias is a valid alias from your minio config file')
    @cmd2.with_argparser(loc_args)
    def do_loc(self, arg):
        "Set context to a particular minio S3 location as described in user minio config file"
        self._navconfig(arg.alias)    
        self.do_lb()

    cb_args = cmd2.Cmd2ArgumentParser()
    cb_args.add_argument('bucket', help='Bucket should be a valid bucket in your current location')
    @cmd2.with_argparser(cb_args)
    def do_cb(self, arg):
        """
        Change to a (new) bucket. This is not treated as just a move to another directory, as the
        notion of a bucket is quite different in S3 from that of a directory. Get used to it.
        """
        bucket = arg.bucket
        if bucket not in self.buckets:
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
    ls_args.add_argument('path', nargs='?',help='Path should be a valid path in your current bucket and location, possibly with a wildcard.')
    @cmd2.with_argparser(ls_args)
    def do_ls(self, arg='/'):
        """ 
        List the files and directories in a bucket, potentially with a wild card.
        """
        if self.path is None: 
            self.path = '/'
        extras = arg.path
        volume, nfiles, ndirs, mydirs, myfiles = self._recurse(self.path, extras)
        self.poutput(_i('Location: ') + self.path + _i(' contains ')+ fmt_size(volume) + _i(' in ') + str(nfiles) + _i(' files/objects.'))
        directory = 'directory' 
        if extras: directory = "match"
        self.poutput(_i(f'This {directory} contains ')+ str(len(myfiles)) + _i(' files and ') + str(len(mydirs)) + _i(' directories.'))
      
        width = arg.width

        if arg.long or arg.metadata or arg.size or arg.date or arg.tags:
            mlen = 0
            for f in myfiles:
                lf = len(f['n'])
                if lf > mlen:
                    mlen = lf
            
            if arg.long or arg.metadata:
                mymetadata = []
                # loop runs with minimum of 32 or the number of processors multiplied by 5, based on Python’s default configuration.
                with ThreadPoolExecutor() as executor:
                    futures = {executor.submit(fetch_metadata, self.client, self.bucket, f): f for f in myfiles}
                    for future in as_completed(futures):
                        try:
                            f, result = future.result()
                            meta = {k[11:]:v for k,v in result.metadata.items() if k.startswith('x-amz-meta')}
                            mymetadata.append((f, meta))
                        except Exception as e:
                            self.poutput(_err('Error fetching metadata {e}'))
                mymetadata = sorted(mymetadata, key=lambda x: x[0]['n'])
            else:
                mymetadata = [(f,None) for f in myfiles]

            for f,meta in mymetadata:
                string = f"{Path(f['n']).name:<{mlen}}  "
                if arg.long or arg.size:
                    string += _e(f"{f['s']:>10}")
                if arg.long or arg.date:
                    string += f"   {f['d']}"
                if arg.tags:
                    string += f"   {f['t']}"
                if arg.long or arg.metadata:
                    string += f"  {meta}"
                self.poutput(string)   
        else:
            self.columnize([f"{Path(f['n']).name}" for f in myfiles],display_width=width)

        if len(mydirs) > 0: 
            if len(mydirs) > 3:
                self.poutput(_i('Sub-directories are : '))
                self.columnize([_e(f'{d[0]}({d[1]})') for d in mydirs],display_width=width)
            else:
                self.poutput(_i('Sub-directories are : ')+_e('  '.join([f'{d[0]}({d[1]})' for d in mydirs])))

    cd_args = cmd2.Cmd2ArgumentParser()
    cd_args.add_argument('path', nargs='?',help='Path should be a valid path in your current bucket and location.')
    @cmd2.with_argparser(cd_args)
    def do_cd(self,line):
        """
        Change default position in bucket to expose contents as if it were a file system directory.
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
        self.buckets.append(bucket_name)
        return self.cb(f'cb {bucket_name}')
    

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
        Rename files within a bucket (server side)
        This is an expensive operation!
        """
        try:
            source, target = tuple(command.targets)
            self.poutput(_i('Command is mv ')+ source+ _i(' to ')+target)
        except:
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

    def default(self,arg):
        if not self.starting:
            self.poutput(_err(f'Command not recognised (at alias={self.alias}, bucket={self.bucket}, path={self.path})'))
        self.starting=False
        self.do_lb()