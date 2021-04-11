import s3fs
import os

bucketname = 'eis-dh-fire'
aws_key_id = os.environ.get( 'AWS_ACCESS_KEY_ID' )
aws_secret_key = os.environ.get( 'AWS_ACCESS_KEY_ID' )
region="us-east-1"

s3f: s3fs.S3FileSystem  = s3fs.S3FileSystem()
print( "Catalog Files:")
print( s3f.ls( f"/{bucketname}/" ) )

