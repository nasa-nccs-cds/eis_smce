import boto3

s3r = boto3.resource('s3')
for bucket in s3r.buckets.all():
     print( bucket.name )


print( s3r.__class__ )

