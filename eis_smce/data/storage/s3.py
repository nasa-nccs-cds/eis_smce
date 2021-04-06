import traitlets.config as tlc
import fnmatch
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import glob, os
import boto3
from intake.source.utils import path_to_glob
# import s3fs

def s3m(): return S3Manager.instance()

class S3Manager(tlc.SingletonConfigurable):

    def __init__( self, **kwargs ):
        tlc.SingletonConfigurable.__init__( self, **kwargs )
        self._client = None
#        self._fs: s3fs.S3FileSystem = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client('s3')
        return self._client

    # @property
    # def fs(self) -> s3fs.S3FileSystem:
    #     if self._fs is None:
    #         self._fs = s3fs.S3FileSystem(anon=False)
    #     return self._fs
    #
    # def get_s3_store(self, bucketname, s3path, modis_filename ):
    #     s3f: s3fs.S3FileSystem  = s3fs.S3FileSystem( anon=True )
    #     store = s3fs.S3Map( root=f"{bucketname}/{s3path}/{modis_filename}_test1", s3=s3f, check=False, create=True )
    #     return store

    def _parse_urlpath( self, urlpath: str ) -> Tuple[str,str]:
        ptoks = urlpath.split(":")[-1].strip("/").split("/")
        return ( ptoks[0], "/".join( ptoks[1:] ) )

    def download( self, data_url: str, destination: str, refresh = True ):
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

    def get_file_list(self, urlpath: str ) -> List[Dict]:
        from intake.source.utils import reverse_format
        s3 = boto3.resource('s3')
        (bucketname, pattern) = self._parse_urlpath( urlpath )
        print( f"get_file_list: urlpath={urlpath}, bucketname={bucketname}, pattern={pattern}")
        gpattern = path_to_glob( pattern )
        files_list = []
        for bucket in s3.buckets.all():
            if fnmatch.fnmatch(bucket.name,bucketname):
                for obj in bucket.objects.all():
                    if fnmatch.fnmatch( obj.key, gpattern ):
                        try:
                            metadata = reverse_format( pattern, obj.key )
                            metadata['resolved'] = f"s3://{bucketname}/{obj.key}"
                            files_list.append(metadata)
                        except ValueError:
                            pass
        return files_list