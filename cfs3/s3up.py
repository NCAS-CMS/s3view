from pathlib import Path

from cfs3.s3core import get_client, sanitise_metadata, desanitise_metadata
import os
import time
import inspect
from functools import wraps
import logging

def mirror_signature(reference):
    """Make the wrapped function adopt the same signature as `reference`."""
    def decorator(func):
        @wraps(reference)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        wrapper.__signature__ = inspect.signature(reference)
        return wrapper
    return decorator

class Uploader:

    def __init__(self, alias, 
                    minio_config='~/.mc/config.json', 
                    default_bucket=None, 
                    verification=None):
        """
        Initialise uploader with the endpoint alias,location of
        the mninio config (if non-standard) and  default bucket
        and verification options.

        Args:
            alias (str): an alias for a standard minio configuration file 
            minio_config (str, optional): the location of your minio config file
            default_bucket (str, optional): bucket where uploads go if no bucket is specified. Defaults to None.
            verification (int, optional): Whether or not to attempt any sort of verification. Deafults to 0 (none).
                verification = 0 : no verification
                verification = 1 : verify size of the uploaded object corresponds to local object
                other forms of verification (checksums etc, not yet supported) 
        """
        self.logger = logging.getLogger(f'cfs3.Uploader[{alias}]')
        self.client = get_client(alias)
        self.bucket = default_bucket
        self.verify = verification
        self.logger.debug('Initialised Uploader')

    def upload_file(self, file_path, bucket=None, metadata=None, object_name = None):
        """
        Upload a file to the location stored when initialised.
        If bucket is None, use the default bucket. If no bucket, raise an error.
        
        Args:
            file_path (str or Path instance): full file location 
            bucket (str, optional): Target bucket. Defaults to None.
            metadata (dict, optional): Metadata to accompany object. Defaults to None.
            object_name (str, optional): Name to use in object store. Defaults to None (use file_path.name)
        """

        if not isinstance(file_path,Path):
            file_path = Path(file_path)
        if not file_path.exists:
            raise FileExistsError(f"Can't find {file_path}")
        
        if object_name is None:
            object_name = file_path.name

        if bucket is None:
            if self.bucket:
                bucket = self.bucket
            else:
                raise ValueError('Cannot upload without a target bucket')

        #make the bucket if it does not exist
        try:
            found = self.client.bucket_exists(bucket)
        except:
            print('** CHECK ENDPOINT ADDRESS and SECURE OPTION')
            raise 
        if not found:
            ok = self.client.make_bucket(bucket)
            if not ok:
                raise RuntimeError(f'Unable to make bucket {bucket}')
            print('Created bucket', bucket)

        #upload
        try:
            if metadata is not None:
                metadata = sanitise_metadata(metadata)
            e1 = time.time()
            result = self.client.fput_object(bucket, object_name, file_path, metadata=metadata)
            e2 = time.time()
            etag = result.etag
            if self.verify:
                size = Path(file_path).stat().st_size
                self.do_verify(size, self.verify, etag, bucket, object_name, metadata)
            e3 = time.time()
        except:
            raise
        self.logger.info(f'Upload time for {object_name} was {e2-e1:.2f}s (with verification {e3-e2:.2f}s')


    def upload_files(self, globstring, bucket, meta_func=None, objName_func=None):
        """ 
        Upload file which match globstring, and if provided, use meta_func to
        create/extract metadata, and also if provided, use objName_func to
        create object names for each file which matches the globstring.
        """
        paths = Path.glob(globstring)
        for path in paths:
            metadata = meta_func(path) if meta_func else None
            objname = objName_func(path) if objName_func else None
            self.upload_file(path, bucket=bucket, metadata=metadata, object_name=objname)


    @mirror_signature(upload_file)
    def move_file_to_s3(self, *args, **kwargs):
        """ 
        Same as upload file, excpet we force a minimum of verification=1 because in this
        case we remove the file when it has been uploaded!
        """
        kwargs['verification'] = max(1,kwargs['verification'])
        self.upload_file(*args, **kwargs)
        os.remove(file_path)


    @mirror_signature(upload_files)
    def move_files_to_s3(self, *args, **kwargs):
        """ 
        Same as upload files, excpet we force a minimum of verification=1 because in this
        case we remove the file when it has been uploaded!
        """
        paths = Path.glob(globstring)
        for path in paths:
            metadata = meta_func(path) if meta_func else None
            objname = objName_func(path) if objName_func else None
            self.move_file_to_s3(path, bucket=bucket, metadata=metadata, object_name=objname)

    def do_verify(self, verify, file_size, etag, bucket, object_name, metadata):
        """ 
        Ideally we verify the file is correct by first checkging the size in bytes
        and then we can try and work out whether or not the etag is correct. But the
        etag is complicated, it's not necessarily the MD5 checksum, so a the moment
        the second step is not done. A warning is raised.
        """
        result = self.client.stat_object(bucket, object_name)
        object_size = result.size
        if object_size != file_size:
            raise RuntimeError(f'Object size ({object_size}) does not match file size ({file_size})')
       
        if metadata is not None:
            ometa = {k[11:]:v for k,v in result.metadata.items() if k.startswith('x-amz-meta')}
            ometa = desanitise_metadata(ometa)
            umeta = desanitise_metadata(metadata)
            for k in ometa:
                try:
                    if ometa[k]!=umeta[k]: 
                        raise RuntimeError(f'Metadata not preserved - u{umeta} - o {ometa}')
                except KeyError as e:
                    raise RuntimeError(f'Metadata not preserved - u{umeta} - o {ometa} (error{e})')

        if verify > 1:
            raise NotImplementedError
            # print(f'Warning - Cannot verify using checksums - but at least file sizes do match for: {object_name}!')
            # see this useful stackoverflow: 
            # https://stackoverflow.com/questions/62555047/how-is-the-minio-etag-generated