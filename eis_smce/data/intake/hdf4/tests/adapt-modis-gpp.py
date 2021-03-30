
import os, xarray as xr
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from intake_xarray.xzarr import ZarrSource

base_dir = "/css/modis/Collection6/L3"
cache_dir = "/att/nobackup/tpmaxwel/ILAB/scratch"
collection = "MOD17A2H-GPP"
batch = "2000/049"
data_url = f"file:/{base_dir}/{collection}/{batch}/*.hdf"
output_dir = f"{cache_dir}/{collection}/{batch}"
os.makedirs( output_dir, exist_ok=True )
part_index: int = 0

h4s: HDF4Source = HDF4Source( data_url  )                        # Creates source encapsulating all matched files in data_url
ds0: xr.Dataset = h4s.read_partition( part_index )               # Each partition corresponds to a single file, downloads file from s3 to local cache before reading.

remote_input_file: str = ds0.attrs['remote_file']
local_input_file: str  = ds0.attrs['local_file']
file_name = os.path.splitext( os.path.basename( local_input_file ) )[0]
result_zarr_file: str = f"file:/{output_dir}/{file_name}.zarr"

print(f"\n HDF4Source DATASET DS0:" )
print( f"\n ***  attributes:")
for vid, v in ds0.attrs.items():
    print(f" ----> {vid}: {v}")
print( f"\n ***  variables:")
for vid, v in ds0.variables.items():
    print(f" ----> {vid}{v.dims} ({v.shape})")

xzSource: ZarrSource = h4s.export( result_zarr_file )            # Exports the current partition (index = 0), Zarr is the default export format

print( f"Exported file '{remote_input_file}' (cached at '{local_input_file}') to '{result_zarr_file}'")
print( "Catalog entry:" )
print( xzSource.yaml() )