import boto3
from pyhdf.SD import SD, SDC, SDS
import os

modis_s3_item = 'mod14/raw/MOD14.A2020296.0645.061.2020348134049.hdf'
bucketname = 'eis-dh-fire'
local_cache_dir = "/home/jovyan/cache"
client = boto3.client('s3')

file_name = modis_s3_item.split("/")[-1]
modis_filepath = os.path.join( local_cache_dir, file_name )
client.download_file( bucketname, modis_s3_item, modis_filepath )

sd = SD( modis_filepath, SDC.READ )
print( f"METADATA keys = {sd.attributes().keys()} ")
print( f"DATASET keys = {sd.datasets().keys()} ")

DATAFIELD_NAME='FP_T31'
data3D = sd.select(DATAFIELD_NAME)
print( data3D.__class__ )
data = data3D[:,:,:]
print( data.__class__ )

# Read geolocation dataset.
lat = sd.select('FP_latitude')
latitude = lat[:,:]
lon = sd.select('FP_longitude')
longitude = lon[:,:]