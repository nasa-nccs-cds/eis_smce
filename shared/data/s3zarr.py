import boto3
import io, os, sys
from pyhdf.SD import SD, SDC
from botocore.exceptions import ClientError
import zarr, s3fs

s3path = 'mod14/raw'
modis_filename = 'MOD14.A2020296.0645.061.2020348134049.hdf'
bucketname = 'eis-dh-fire'
local_cache_dir = "/home/jovyan/cache"

itemname = f"{s3path}/{modis_filename}"
modis_filepath = f"{local_cache_dir}/{modis_filename}"
zarr_filename = f"{os.path.splitext(modis_filename)[0]}.zarr"
zarr_filepath = f"{local_cache_dir}/{zarr_filename}"

s3f: s3fs.S3FileSystem  = s3fs.S3FileSystem()
store = s3fs.S3Map( root=s3path, s3=s3f, check=False )

s3 = boto3.client('s3')
s3.download_file( bucketname, itemname, modis_filepath )
modis_sd: SD = SD( modis_filepath, SDC.READ )

root = zarr.group(store=store)
for (akey, aval) in modis_sd.attributes().items():
    if akey.startswith("CoreMetadata"): pass
    else: root.attrs[akey] = aval

for (dskey, dsval) in modis_sd.datasets().items():
    zarr.copy( dsval, root, log=sys.stdout)






# print( f"Read MODIS FILE {modis_filename}, attrs:")
# for (akey, aval) in modis_sd.attributes().items():
#     print(f" -> {akey}: {aval}")
#
# print( f"\nDatasets:")
# for (dskey, dsval) in modis_sd.datasets().items():
#     print(f" -> {dskey}: {dsval}")


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