import boto3

bucketname = 'eis-dh-fire'
s3 = boto3.client('s3')

response = s3.list_buckets()
print( "\nCurrent eis buckets:")
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')

print( f"\nUploading script to bucket: {bucketname}\n")
response = s3.upload_file( __file__, bucketname, 'mod14/raw/test_script.py' )

print( response )