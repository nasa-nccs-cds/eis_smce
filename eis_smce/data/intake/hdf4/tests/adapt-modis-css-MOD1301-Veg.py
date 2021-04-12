import os, xarray as xr
import intake

bucket_id = "eis-dh-fire"
base_dir = "/css/modis/Collection6/L3/"
cache_dir = "/att/nobackup/tpmaxwel/ILAB/scratch"
collection = "MOD13Q1-Vegetation"
year = "2001"
day = "0*"
file_names = "MOD13Q1.{sample}.h09v09.006.{sid}.hdf"
cat_path = f"{cache_dir}/catalog"
batch = f"{year}/{day}/{file_names}"
output_file = f"{cache_dir}/{collection}/h09v09.zarr"
for path in [ cat_path, os.path.dirname(output_file) ]:  os.makedirs( path, exist_ok=True )
data_url = f"file://{base_dir}/{collection}/{batch}"

if __name__ == '__main__':

    h4s = intake.open_hdf4( data_url )
    h4s.export( output_file, bucket=bucket_id )




