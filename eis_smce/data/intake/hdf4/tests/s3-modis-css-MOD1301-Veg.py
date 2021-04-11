import os, xarray as xr
import intake

base_dir = "/css/modis/Collection6/L3/"
cache_dir = "/att/nobackup/tpmaxwel/ILAB/scratch"
collection = "MOD13Q1-Vegetation"
bucketname = 'eis-dh-fire'
year = "2001"
day = "0*"
file_names = "MOD13Q1.{sample}.h09v09.006.{sid}.hdf"
cat_path = f"s3://{bucketname}/catalog"
batch = f"{year}/{day}/{file_names}"
output_file = f"s3://{bucketname}/{collection}/h09v09.zarr"
data_url = f"file://{base_dir}/{collection}/{batch}"

if __name__ == '__main__':

    h4s = intake.open_hdf4( data_url, cache_dir=cache_dir )
    h4s.export( output_file, cat_path=cat_path )




