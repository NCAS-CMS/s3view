import cf 
from cfs3.s3up import Uploader
from cfs3.cfchunking import get_optimal_chunkshape
from cfs3.logging_utils import get_logger
from pathlib import Path
import json

class CFSplitter:

    def __init__(self, 
                filename_handler=None, 
                meta_handler=None,
                output_folder=''
                ):
        self.logger = get_logger(__name__)

        if filename_handler:
            self.filename_handler = filename_handler
        else:
            self.filename_handler = self._default_filenames

        if meta_handler:
            self.meta_handler = meta_handler
        else:
            self.meta_handler = self._default_metadata

        if output_folder=='':
            self.output_folder = Path.getcwd()
        else:
            self.output_folder = Path(output_folder)
        self.logger.debug(f'Instantiated CFSPlitter, writing to {str(output_folder)}')
     

    def split_one(self,filename, with_json=True, uncompressed_chunk_volume_MB=4):
        """ 
        Split one file into constituent fields and create a file per field and
        (if required) an accompanying json file of b-metadata to be used for
        metadata upload.
        """
        self.logger.debug(f'Splitting {filename}')
        # we use this to guide the chunking algorithm
        chunk_volume = uncompressed_chunk_volume_MB * 1e6

        # this forces the output chunking for the coordinates to be 4MB
        # which is big enough for most conceivable situations.
        fields = cf.read(filename, store_dataset_chunks=False)
        filename = Path(filename)

        nfiles = len(fields)
        stems = []
        self.nfiles = nfiles
        for ith,field in enumerate(fields):

            #self.logger.debug(field.dump(display=False))

            chunk_shape = get_optimal_chunkshape(field, chunk_volume, logger=self.logger)
            self.logger.debug(f'Setting chunkshape {chunk_shape} for {field.identity()} in [{filename.name}]')
            field.data.nc_set_dataset_chunksizes(chunk_shape)

            self.logger.debug('Going to metadata handler')
            metadata, field = self.meta_handler(filename, field)
            self.logger.debug('Going to filename handler')
            output_filename = self.output_folder/self.filename_handler(filename, field, metadata) 
            
            ncout = output_filename.with_suffix('.nc')
            jfout = output_filename.with_suffix('.json')
            self.logger.debug(f'Writing {ith+1}/{nfiles} file: {ncout.stem}')
            cf.write([field,],ncout)
            self.logger.debug(f'Written {ncout}')
            
            if with_json and metadata:
                with open(jfout,'w') as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)
            stems.append(output_filename)
        
        return stems

    def _default_metadata(self, filename, field):
        """ 
        Generate some default metadata for output 
        """

        metadata = {k:v for k,v in field.properties().items()}
        return metadata, field


    def _default_filenames(self, filename, field, metadata):
        """ 
        Provides the simplest possible name for the output file. 
        """
        if self.nfiles > 1:
            ncname = field.nc_get_variable()
            
            return f"{ncname}_{filename.stem}-split"
        else:
            return f"{filename.stem}-split"


class CFuploader (CFSplitter):

    def __init__(self, alias, bucket, *args, **kwargs):
        super(self).__init__(*args,**kwargs)
        self.uploader=Uploader(alias)
        self.bucket = bucket

    def simple_upload(self, filename, parallel_upload=False, uncompressed_chunk_volume_MB=4):
        """ 
        Uploads the CF fields from a singe file as independent files in the object store.
        """

        stems = self.split_one(filename, uncompressed_chunk_volume_MB=uncompressed_chunk_volume_MB)

        for stem in stems:
            filename = stem+'.nc'
            metadata = json.load(stem+'.json')
            # this will delete the field that was written!
            self.uploader.move_file_to_s3(filename, self.bucket, metadata=metadata)


class MetaFix:
    def __init__(self, external_metadata):
        """ 
        External metadata is the metadata we want to add or fix from the original field
        and return for use as metadata outside the file. We define it using a dictionary.
        The expectation is that values for the metadata will be returned by the
        apply method as a dictionary, and that where the external metadata is different
        from the field metadata, we fix the field metadata. The only exception to that
        is where the external_metadata definition at instantiation has a value of
        None, in which case the expectation is that the value will be _obtained_ from
        the field (not corrected). 
        
        For example:
           external_metadata = {'project':'cmip6','experiment':'dummy2','standard_name':None}
        We would expect that if the field metadata did not have project or experiment,
        we would add it, if it did have either and it was different, we would overwrite it, and
        we are hopefully extracting the standard name from the field and returning it 
        to ouput metadata.
        """
        self.external = external_metadata
    def __call__(self, filename, field):
        properties = field.properties()
        output_metadata = {k:v for k,v in self.external.items()}
        for k,v in properties.items():
            if  k in output_metadata:
                ov = output_metadata[k]
                if ov is None:
                    output_metadata[k]=v
                else:
                    field.set_property(k,ov)
        for k,v in output_metadata.items():
            if k not in properties and v is not None:
                field.set_property(k,v)
        return output_metadata, field 
                
class FileNameFix:
    def __init__(self, drs, filename_map=None, splitter=None):
        """
        Instantiate with a DRS list, and if splitting (see the 
        call method documentation), a filename_map to be used
        to map the parts of the filename onto terms. To split
        using a more complicated method than just split('_'),
        pass a function for that!
        """
        self.drs = drs
        self.splitter=splitter
        self.filename_map=filename_map

    def __call__(self, filename, field, metadata=None):
        """ 
        Calculate an appropriate filename for an output file.
        
        The algorithm used 
        1. parses the provided DRS for terms which start with !, these are calculated
        from the field (details of the options there are discussed below).
        2. extracts any DRS values which are keys in the provided metadata, and
        3. if self.filename_map is not None, looks for the other DRS values from the 
        filename (the method is discussed below).

        For step 1: we understand 
        - !ncname, which will extract the netcdf variable name associated with the field.
        - !freq, which will attempt to use the cell method and cell bounds to establish
        a frequency.

        For step 3: if self.filename_map is not None, the provided filename is split 
        using the splitter function, and DRS terms are extracted from the resulting
        dictionary. 
       
        """
        results = {}

        # step 1
        internals = [d for d in self.drs if d.startswith('!')]
        for i in internals:
            match i: 
                case '!ncname': 
                    results[i] = field.nc_get_variable()
                case '!freq':
                    results[i] = ''.join([str(x) for x in list(self._get_freq(field))])
                case _:
                    raise ValueError(f'Unknown DRS term {i}')

        # step 2
        if metadata:
            meta = {k:v for k,v in metadata.items() if k in self.drs and not k.startswith('!')}
            for k,v in meta.items():
                results[k]=v

        # step 3
        if self.filename_map is not None:
            if self.splitter is None:
                parts = str(filename.stem).split('_')
            else:
                parts = self.splitter(filename)
            if len(parts) != len(self.filename_map):
                print(f'filename parts are {parts}')
                print(f'filename map expected {self.filename_map}')
                raise ValueError(f'Filename [{filename.stem}] cannot be split onto the expected map.')
            splitvals = {fm:p for fm,p in zip(self.filename_map,parts) if fm in self.drs}
            for k,v in splitvals.items():
                results[k]=v

        try:
            result = [results[k] for k in self.drs]
        except Exception as e:
            raise e
        
        result = '_'.join(result)
        return result


    def _get_freq(self,field):
        """ 
        Infer frequency interval from time-coordinate and use 
        cell methods to discriminate between mean and instaneous.
        """
        try:
            tc = field.dimension_coordinate('time', None)
        except ValueError:
            # Assume fixed data
            return '','fx'
        
        td = tc.data
        delta = tc[1].data-tc[0].data
        delta.Units = cf.Units('day')
        
        if delta < cf.TimeDuration(1,'day'):   #hours
            delta.Units = cf.Units('hour')
            return int(delta),'h'
        elif delta < cf.TimeDuration(28,'day'): 
            return int(delta),'d'
        elif delta < cf.TimeDuration(31,'day'):
            return 1,'m'
        elif delta> cf.TimeDuration(89,'day') and delta < cf.TimeDuration(93,'day'):
            return 3,'m'
        elif delta> cf.TimeDuration(359,'day') and delta < cf.TimeDuration(367,'day'): 
            return 1,'y'
        else:
            if td.calendar == '360_day':
                return int(delta/360),'y'
            else:
                return int(delta/360.25),'y'


        
        

        




