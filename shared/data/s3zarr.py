import boto3

s3 = boto3.client('s3')

for bucket in s3.buckets.all():
    print(bucket.name)