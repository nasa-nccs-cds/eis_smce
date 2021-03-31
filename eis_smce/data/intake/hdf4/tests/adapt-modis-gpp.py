
import os, xarray as xr
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from intake_xarray.xzarr import ZarrSource

base_dir = "/css/modis/Collection6/L3"
cache_dir = "/att/nobackup/tpmaxwel/ILAB/scratch"
collection = "MOD17A2H-GPP"
batch = "2000/049/MOD17A2H.A2000049.h11{v}.006.{ts}.hdf"
data_url = f"file:/{base_dir}/{collection}/{batch}/*.hdf"
output_dir = f"{cache_dir}/{collection}/{batch}"
os.makedirs( output_dir, exist_ok=True )
part_index: int = 0

h4s: HDF4Source = HDF4Source( data_url  )                              # Creates source encapsulating all matched files in data_url
ds0: xr.Dataset = h4s.read_partition( part_index )                     # Each partition corresponds to a single file, downloads file from s3 to local cache before reading.

print(f"\n HDF4Source DATASET DS0:" )
print( f"\n ***  attributes:")
for vid, v in ds0.attrs.items():
    print(f" ----> {vid}: {v}")
print( f"\n ***  variables:")
for vid, v in ds0.variables.items():
    print(f" ----> {vid}{v.dims} ({v.shape})")

stores = h4s.export( location=f"file:/{output_dir}" )    # Exports all partitions, Zarr is the default export format

print( f"Exported file '{ds0.attrs['remote_file']}' (cached at '{ds0.attrs['local_file']}') to '{output_dir}'")
print( "Sample catalog entry:" )
print( ZarrSource( stores[0] ).yaml() )