import boto3

itemname = 'mod14/raw/MOD14.A2020296.0645.061.2020348134049.hdf'

s3 = boto3.resource('s3')

for bucket in s3.buckets.all():
    if bucket.name.startswith("eis"):
        print(f'{bucket.name}:')
#        for obj in bucket.objects.all():
 #           print(f'{obj.key}: {obj.__class__}')


#obj = s3.Object(bucketname, itemname)
#body = obj.get()['Body'].read()