import boto3
from intake.source.utils import reverse_format

bucketname = 'eis-dh-fire'
s3 = boto3.resource('s3')
pattern = "mod14/raw/MOD14.{date}.{tod}.061.{ts}.hdf"

files_list = []
for bucket in s3.buckets.filter( Bucket= bucketname ):
    if bucket.name == bucketname:
        print(f'** {bucket.name}:')
        for obj in bucket.objects.all():
            try:
                metadata = reverse_format( pattern, obj.key )
                metadata['resolved'] = obj.key
                files_list.append( metadata )
            except ValueError: pass


for entry in files_list:
    print( entry )



# for bucket in s3.buckets.all():
#    if bucket.name.startswith("eis"):
#        print(f'** {bucket.name}:')
#        for obj in bucket.objects.all():
#           print(f'   -> {obj.key}: {obj.__class__}')

# print( f"\nUploading script to bucket: {bucketname}\n")
# response = s3.upload_file( __file__, bucketname, 'mod14/raw/test_script.py' )
#
# print( response )