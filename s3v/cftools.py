import cf 
from s3v.s3up import Uploader
from s3v.cfchunking import get_optimal_chunkshape
from s3v.logging_utils import get_logger
import logging
import os
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

            self.logger.debug(f'Going to metadata handler')
            metadata = self.meta_handler(filename, field)
            self.logger.debug(f'Gooing to filename handler')
            output_filename = self.output_folder/self.filename_handler(filename, field)
            
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
        return metadata


    def _default_filenames(self, filename, field):
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


