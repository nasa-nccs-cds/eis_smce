import traitlets.config as tlc
import fnmatch
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import glob, os
import boto3, intake
from fsspec.mapping import FSMap
from intake.source.utils import path_to_glob
import s3fs

def s3m(): return S3Manager.instance()
def has_char(string: str, chars: str): return 1 in [c in string for c in chars]

class S3Manager(tlc.SingletonConfigurable):

    def __init__( self, **kwargs ):
        tlc.SingletonConfigurable.__init__( self, **kwargs )
        self._client = None
        self._fs: s3fs.S3FileSystem = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client('s3')
        return self._client

    @property
    def fs(self) -> s3fs.S3FileSystem:
        if self._fs is None:
            self._fs = s3fs.S3FileSystem()
        return self._fs

    def set_acl(self, path: str ):
        key = self.item_key(path)
        itemlist = list( self._fs.ls( key ) )
        print(f" itemlist = {itemlist} " )
        print( f"Setting ACL on key {key}")
        self._fs.chmod( key, "bucket-owner-full-control" )

    def store(self, bucketname: str, s3path: str = "" ) -> FSMap:
         store: FSMap = s3fs.S3Map( root=f"{bucketname}/{s3path}", s3=self.fs, check=False, create=True )
         return store

    def item_path(self, path: str) -> str:
        return path.split(":")[-1].replace("//", "/").replace("//", "/")

    def item_key(self, path: str) -> str:
        return path.split(":")[-1].strip("/")

    def parse(self, urlpath: str) -> Tuple[str, str]:
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
        (bucketname, pattern) = self.parse(urlpath)
        print( f"get_file_list: urlpath={urlpath}, bucketname={bucketname}, pattern={pattern}")
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
                            print( f" Metadata processing error: {err}, Did you mix glob and pattern in file name?")
        return files_list