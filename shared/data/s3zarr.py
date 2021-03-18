import boto3

s3 = boto3.resource('s3')

for bucket in s3.buckets.all():
    if bucket.name.startswith("eis"):
        print(f'{bucket.name}:')
        for obj in bucket.objects.all():
            print(f'{obj.key}: {obj.__class__}')