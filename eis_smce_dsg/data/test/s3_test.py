import boto3

default_bucketname = 'eis-dh-fire'
dev_bucketname = 'eis-dh-fire-dev'

s3 = boto3.client('s3')
response = s3.list_buckets()

print( "\nCurrent eis buckets:")
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')

print( f"\nCreating bucket: {dev_bucketname}\n")
s3.create_bucket(Bucket=dev_bucketname)

print( "DONE")