import boto3
from eis_smce.data.storage.s3 import s3m

bucketname = 'eis-dh-hydro'
# prefix = "catalog/MOD13Q1"
prefix = 'projects/eis_freshwater/'
s3r = boto3.resource('s3')

print( "Catalog objects:")
bucket = s3r.Bucket(bucketname)
for obj in bucket.objects.filter( Prefix=prefix ):
     bucket.
     print( f"s3://{obj.bucket_name}/{obj.key}" )

