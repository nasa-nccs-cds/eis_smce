import traitlets.config as tlc
import boto3, s3fs

class S3Manager(tlc.SingletonConfigurable):

    def __init__( self, **kwargs ):
        tlc.SingletonConfigurable.__init__(**kwargs)
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
            self._fs = s3fs.S3FileSystem(anon=False)
        return self._fs

    def get_s3_store(self, bucketname, s3path, modis_filename ):
        s3f: s3fs.S3FileSystem  = s3fs.S3FileSystem( anon=True )
        store = s3fs.S3Map( root=f"{bucketname}/{s3path}/{modis_filename}_test1", s3=s3f, check=False, create=True )
        return store