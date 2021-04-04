
import os, xarray as xr
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from intake_xarray.xzarr import ZarrSource

base_dir = "/att/nobackup/mcarrol2/MODIS"
cache_dir = "/att/nobackup/tpmaxwel/ILAB/scratch"
collection = "MCD12Q1"
location = "h09v09"
batch = f"200?/001/MCD12Q1.A200?001.{location}.006.*.hdf"
output_file = f"{cache_dir}/{collection}/{location}.zarr"
os.makedirs( os.path.dirname(output_file), exist_ok=True )
part_index: int = 0

data_url = f"file:/{base_dir}/{collection}/{batch}"
h4s: HDF4Source = HDF4Source( data_url  )                              # Creates source encapsulating all matched files in data_url
ds0: xr.Dataset = h4s.read_partition( part_index )                     # Each partition corresponds to a single file, downloads file from s3 to local cache before reading.

print(f"\n HDF4Source DATASET DS0:" )
print( f"\n ***  attributes:")
for vid, v in ds0.attrs.items():
    print(f" ----> {vid}: {v}")
print( f"\n ***  variables:")
for vid, v in ds0.variables.items():
    print(f" ----> {vid}{v.dims} ({v.shape})")

store = h4s.export( f"file:/{output_file}" )

print( f"Exported file '{ds0.attrs['remote_file']}' (cached at '{ds0.attrs['local_file']}') to '{store}'")
print( "Sample catalog entry:" )
print( ZarrSource( store ).yaml() )