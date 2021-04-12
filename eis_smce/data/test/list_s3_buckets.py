import boto3

bucketname = 'eis-dh-fire'

s3r = boto3.resource('s3')
for bucket in s3r.buckets.all():
     print( bucket.name )

print( "Catalog objects:")
bucket = s3r.Bucket(bucketname)
for obj in bucket.objects.filter(Prefix="catalog"):
     print( f"s3://{obj.bucket_name}/{obj.key}" )

