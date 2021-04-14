import boto3
from eis_smce.data.storage.s3 import s3m

bucketname = 'eis-dh-fire'
# prefix = "catalog/MOD13Q1"
prefix = 's3://eis-dh-hydro/projects/eis_freshwater/'
s3r = boto3.resource('s3')

print( "Catalog objects:")
bucket = s3r.Bucket(bucketname)
for obj in bucket.objects.filter( Prefix=prefix ):
     print( f"s3://{obj.bucket_name}/{obj.key}" )
     s3m().set_acl(obj.key)

