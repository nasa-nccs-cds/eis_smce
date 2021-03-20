import boto3
import io, os, sys
import zarr, s3fs
from eis_smce_dsg.shared.data.s3zarr import zarrModisDS

modis_s3_item = 'mod14/raw/MOD14.A2020296.0645.061.2020348134049.hdf'
bucketname = 'eis-dh-fire'
local_cache_dir = "/home/jovyan/cache"
store_dir = f'{local_cache_dir}/{os.path.splitext(modis_s3_item)[0]}.zarr'
os.makedirs( store_dir, exist_ok=True )

s3 = boto3.client('s3')
store = zarr.DirectoryStore( store_dir )

zds = zarrModisDS( s3, store, local_cache_dir )
zds.from_s3( bucketname, modis_s3_item )

print( f"DONE")


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



