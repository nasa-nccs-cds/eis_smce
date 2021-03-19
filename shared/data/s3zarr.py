import boto3
import io, h5py
import s3fs
import xarray as xr

itemname = 'mod14/raw/MOD14.A2020296.0645.061.2020348134049.hdf'
bucketname = 'eis-dh-fire'

# s3 = boto3.resource('s3')

# for bucket in s3.buckets.all():
#    if bucket.name.startswith("eis"):
#        print(f'{bucket.name}:')
#        for obj in bucket.objects.all():
#           print(f'{obj.key}: {obj.__class__}')


# obj = s3.Object(bucketname, itemname)
# hdf_bytes = obj.get()['Body'].read()
# f = io.BytesIO(hdf_bytes)
# h = h5py.File(f,'r')
# ds: xr.Dataset = xr.open_dataset( f, engine='h5py' )


s3 = s3fs.S3FileSystem()
f = h5py.File( s3.open(f"s3://{bucketname}/{itemname}", "rb") )

print( f"READ ds:")