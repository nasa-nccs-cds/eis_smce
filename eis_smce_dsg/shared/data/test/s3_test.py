import boto3

default_bucketname = 'eis-dh-fire'
dev_bucketname = 'eis-dh-fire-dev'

s3 = boto3.client('s3')
response = s3.list_buckets()

print( "\nCurrent eis buckets:")
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')

print( f"\nBucket policy for {default_bucketname}:")
result = s3.get_bucket_policy(Bucket=default_bucketname)
print(result['Policy'])

print( f"\nCreating bucket: {dev_bucketname}\n")
s3.create_bucket(Bucket=dev_bucketname)

print( "DONE")