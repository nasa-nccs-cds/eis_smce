import boto3

bucketname = 'eis-dh-fire'
s3 = boto3.client('s3')

for bucket in s3.buckets.all():
   if bucket.name.startswith("eis"):
       print(f'{bucket.name}:')
       for obj in bucket.objects.all():
          print(f'{obj.key}: {obj.__class__}')

print( f"\nUploading script to bucket: {bucketname}\n")
response = s3.upload_file( __file__, bucketname, 'mod14/raw/test_script.py' )

print( response )