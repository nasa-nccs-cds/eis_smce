import boto3

bucket_id = "eis-dh-fire"
s3 = boto3.resource( 's3' )

bucket = s3.Bucket( bucket_id )
for obj in bucket.objects.all():
    print(obj)