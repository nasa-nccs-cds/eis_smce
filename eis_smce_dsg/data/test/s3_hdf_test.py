import boto3
from pyhdf.SD import SD, SDC, SDS
import os

modis_s3_item = 'mod14/raw/MOD14.A2020298.1835.061.2020348153757.hdf'
bucketname = 'eis-dh-fire'
local_cache_dir = "/home/jovyan/cache"
client = boto3.client('s3')

file_name = modis_s3_item.split("/")[-1]
modis_filepath = os.path.join( local_cache_dir, file_name )
if not os.path.exists(modis_filepath): client.download_file( bucketname, modis_s3_item, modis_filepath )
print( f"Reading file {modis_filepath}")

sd = SD( modis_filepath, SDC.READ )
print( f"METADATA keys = {sd.attributes().keys()} ")
print( f"DATASET keys = {sd.datasets().keys()} ")

DATAFIELD_NAME='FP_T31'
sds: SDS = sd.select(DATAFIELD_NAME)
print( f"FP_T31 dimensions = {sds.dimensions()}" )
print( f"FP_T31 attributes = {sds.attributes().keys()}" )

sample: SDS = sd.select('FP_sample')
print( f"sample data = {sample[:]}" )

line: SDS = sd.select('FP_line')
print( f"line data = {line[:]}" )

# Read geolocation dataset.
lat_sds: SDS = sd.select('FP_latitude')
print( f"latitude dimensions = {lat_sds.dimensions()}" )
print( f"latitude attributes = {lat_sds.attributes().keys()}" )

lon_sds: SDS = sd.select('FP_longitude')
print( f"longitude dimensions = {lon_sds.dimensions()}" )
