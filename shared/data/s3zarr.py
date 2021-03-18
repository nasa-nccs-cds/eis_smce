import boto3
# from boto3.resources.base import ServiceResource

s3 = boto3.resource('s3')

for bucket in s3.buckets.all():
    if bucket.name.startswith("eis"):
        print(f'{bucket.name}:')
        for obj in bucket.objects:
            print(f'{obj.key}: {obj.__class__}')