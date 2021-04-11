import s3fs
import os
#aws_key_id = os.environ.get( 'AWS_ACCESS_KEY_ID' )
#aws_secret_key = os.environ.get( 'AWS_ACCESS_KEY_ID' )
#region="us-east-1"

bucketname = 'eis-dh-fire'
source = "MOD13Q1-Vegetation"
s3f: s3fs.S3FileSystem  = s3fs.S3FileSystem()

print( "Data Files:")
print( s3f.ls( f"/{bucketname}/{source}" ) )

print( "Catalog Files:")
cat_bucket = f"s3://{bucketname}/catalog"
files = s3f.ls(cat_bucket)
print( list(files) )
for file in files:
    s3f.download( file, f"/tmp/catalog/{os.path.basename(file)}" )


