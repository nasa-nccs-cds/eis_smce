import boto3
import io, os
from pyhdf.SD import SD, SDC
import s3fs
import xarray as xr

itemname = 'mod14/raw/MOD14.A2020296.0645.061.2020348134049.hdf'
bucketname = 'eis-dh-fire'

filename = itemname.split("/")[-1]
filepath = f"/home/jovyan/cache/{filename}"

s3 = boto3.client('s3')
s3.download_file( bucketname, itemname, filepath )
modis_sd: SD = SD( filepath, SDC.READ )

print( f"Read MODIS FILE {filename}, attrs:")
for (akey, aval) in SD.attributes().items():
    print(f" -> {akey}: {aval}")

print( f"\nDatasets:")
for (dskey, dsval) in modis_sd.datasets().items():
    print(f" -> {dskey}: {dsval}")

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

# filename = itemname.split("/")[-1]
# s3f: s3fs.S3FileSystem  = s3fs.S3FileSystem()
# f = s3f.open(f"s3://{bucketname}/{itemname}", "rb")
# f.read()
# f.write( f"~/cache/{filename}")
# modis_sd = SD( f"~/cache/{filename}", SDC.READ )



print( f"READ ds:")