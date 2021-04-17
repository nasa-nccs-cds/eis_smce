from eis_smce.data.common.base import EISSingleton
import fnmatch, s3fs
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import glob, os
import boto3, intake
from collections.abc import MutableMapping
from intake.source.utils import path_to_glob

def s3m(): return S3Manager.instance()
def has_char(string: str, chars: str): return 1 in [c in string for c in chars]

class S3Manager(EISSingleton):

    def __init__( self, **kwargs ):
        EISSingleton.__init__( self, **kwargs )
        self._client = None
        self._resource = None
        self._fs: s3fs.S3FileSystem = None

    @property
    def fs(self) -> s3fs.S3FileSystem:
        if self._fs is None:
            self._fs = s3fs.S3FileSystem( anon=False, s3_additional_kwargs=dict( ACL="bucket-owner-full-control" ) )
        return self._fs

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client('s3')
        return self._client

    @property
    def resource(self):
        if self._resource is None:
            self._resource = boto3.resource('s3')
        return self._resource

    def item_path(self, path: str) -> str:
        return path.split(":")[-1].replace("//", "/").replace("//", "/")

    def item_key(self, path: str) -> str:
        return path.split(":")[-1].strip("/")

    def parse(self, urlpath: str) -> Tuple[str, str]:
        ptoks = urlpath.split(":")[-1].strip("/").split("/")
        return ( ptoks[0], "/".join( ptoks[1:] ) )

    def download( self, data_url: str, destination: str, refresh = False ):
        os.makedirs( destination, exist_ok=True )
        toks = data_url.split("/")
        file_name = toks[-1]
        file_path = os.path.join( destination, file_name )
        if refresh or not os.path.exists( file_path ):
            ibuket = 2 if len(toks[1]) == 0 else 1
            bucketname = toks[ibuket]
            s3_item = '/'.join( toks[ibuket + 1:] )
            client = boto3.client('s3')
            client.download_file( bucketname, s3_item, file_path )
        return file_path

    def get_store(self, path: str ) -> MutableMapping:
        self.fs.delete( path, recursive=True )
        return s3fs.S3Map( root=path, s3=self.fs, check=False, create=True )

    def upload_files(self, src_path: str, dest_path: str ):
        if src_path != dest_path:
            ( bucket, item ) = self.parse( dest_path )
            bucket = self.resource.Bucket(bucket)
            for subdir, dirs, files in os.walk(src_path):
                for file in files:
                    full_path = os.path.join(subdir, file)
                    with open(full_path, 'rb') as data:
                        key = f"{item}/{full_path[len(src_path) + 1:]}"
                        self.logger.info(f"Uploading item: {bucket}:{key}")
                        bucket.put_object( Key=key, Body=data, ACL="bucket-owner-full-control" )

    def get_file_list(self, urlpath: str ) -> List[Dict]:
        from intake.source.utils import reverse_format
        s3 = boto3.resource('s3')
        (bucketname, pattern) = self.parse(urlpath)
        self.logger.info( f"get_file_list: urlpath={urlpath}, bucketname={bucketname}, pattern={pattern}")
        is_glob = has_char(pattern, "*?[")
        gpattern = path_to_glob( pattern )
        files_list = []
        for bucket in s3.buckets.all():
            if fnmatch.fnmatch(bucket.name,bucketname):
                for obj in bucket.objects.all():
                    if fnmatch.fnmatch( obj.key, gpattern ):
                        try:
                            ( okey, opattern) = ( os.path.basename(obj.key), os.path.basename(pattern)) if is_glob else (obj.key, pattern)
                            metadata = reverse_format( opattern, okey )
                            metadata['resolved'] = f"s3://{bucketname}/{obj.key}"
                            files_list.append(metadata)
                        except ValueError as err:
                            self.logger.error( f" Metadata processing error: {err}, Did you mix glob and pattern in file name?")
        return files_list