import os, xarray as xr
import intake

bucket_id = "eis-dh-fire"
base_dir = "/css/modis/Collection6/L3/"
collection = "MOD13Q1-Vegetation"
year = "2001"
day = "*"
file_names = "MOD13Q1.{sample}.h09v09.006.{sid}.hdf"
batch = f"{year}/{day}/{file_names}"
s3_output_file = f"s3://{bucket_id}/{collection}/h09v09-{year}.zarr"
data_url = f"file://{base_dir}/{collection}/{batch}"

if __name__ == '__main__':

    h4s = intake.open_hdf4( data_url )
    h4s.export( s3_output_file, bucket=bucket_id )




