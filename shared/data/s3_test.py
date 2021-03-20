import boto3
import io, os, sys
from pyhdf.SD import SD, SDC
from botocore.exceptions import ClientError
import zarr, s3fs

dev_bucketname = 'eis-dh-fire-dev'

s3 = boto3.client('s3')

print( "Current eis buckets:")
for bucket in s3.buckets.all():
   if bucket.name.startswith("eis"):
       print(f' -> {bucket.name}:')

print( f"\nCreating bucket: {dev_bucketname}")
s3.create_bucket(Bucket=dev_bucketname)

print( "DONE")