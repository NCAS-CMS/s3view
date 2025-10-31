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
                name_generator=None, 
                external_metadata={}, 
                use_internal_bmetadata=False, 
                output_folder=''):
        self.logger = get_logger(__name__)
        self.metadata = external_metadata
        self.use_b = use_internal_bmetadata
        self.name_generator = name_generator
        if output_folder=='':
            self.output_folder = os.getcwd()
        else:
            self.output_folder = output_folder
        self.logger.debug('Instantiated CFSPlitter')
     

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
            metadata = self._parse(field)

            chunk_shape = get_optimal_chunkshape(field, chunk_volume, logger=self.logger)
            self.logger.debug(f'Setting chunkshape {chunk_shape} for {field.identity()} in [{filename.name}]')
            field.data.nc_set_dataset_chunksizes(chunk_shape)
            
            output_filename = self._generate_filename(filename, field)
            output_filename = Path(self.output_folder)/output_filename
            
            ncout = output_filename.with_suffix('.nc')
            jfout = output_filename.with_suffix('.json')
            self.logger.debug(f'Writing {ith+1}/{nfiles} file: {ncout}')
            cf.write([field,],ncout)
            self.logger.debug(f'Written {ncout}')
            
            if with_json and metadata:
                json.dump(metadata, jfout, ensure_ascii=False, indent=4)
            stems.append(output_filename)
        
        return stems

    def _parse(self, field):
        metadata = {}
        properties = field.properties()
        self.logger.debug(f'Field properties {properties}')
        for k,v in properties.items():
            if self.use_b:
                metadata[k] = v
            elif k in self.metadata:
                metadata[k] = v
        return metadata


    def _generate_filename(self, filename, field):

        if self.name_generator:
            name = self.name_generator(filename, field)
        else:
            name = self.default_name(filename, field)
        return name


    def default_name(self, filename, field):
        """ 
        Provides the simplest possible name for the output file. 
        """
        #FIXME, only using the name for now, really want path but need to check pytest permissions
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


