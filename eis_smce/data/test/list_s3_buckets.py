import boto3

bucketname = 'eis-dh-fire'
prefix = "catalog/MOD13Q1.yml"
s3r = boto3.resource('s3')

print( "Catalog objects:")
bucket = s3r.Bucket(bucketname)
for obj in bucket.objects.filter( Prefix=prefix ):
     print( f"s3://{obj.bucket_name}/{obj.key}" )

