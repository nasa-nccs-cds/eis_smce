import s3fs
import os

bucketname = 'eis-dh-fire'
aws_key_id = os.environ.get( 'AWS_ACCESS_KEY_ID' )
aws_secret_key = os.environ.get( 'AWS_ACCESS_KEY_ID' )
region="us-east-1"

s3f: s3fs.S3FileSystem  = s3fs.S3FileSystem( anon=False ) # True, key=aws_key_id, secret=aws_secret_key )

print( s3f.ls( f"/{bucketname}" ) )

#
#     store = s3fs.S3Map( root=f"{bucketname}/{s3path}/{modis_filename}_test1", s3=s3f, check=False, create=True )