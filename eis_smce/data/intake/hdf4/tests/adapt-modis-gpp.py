
import os, xarray as xr
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from intake_xarray.xzarr import ZarrSource

base_dir = "/css/modis/Collection6/L3"
cache_dir = "/att/nobackup/tpmaxwel/ILAB/scratch"
collection = "MOD17A2H-GPP"
batch = "2000/049/MOD17A2H.A2000049.h11{v}.006.{ts}.hdf"
file = "2000/049/MOD17A2H.A2000049.h11v04.006.2015136155345.hdf"
output_dir = os.path.dirname(f"{cache_dir}/{collection}/{batch}")
output_file = f"{cache_dir}/{collection}/{file}"
os.makedirs( output_dir, exist_ok=True )
part_index: int = 0

data_url = f"file:/{base_dir}/{collection}/{file}"
h4s: HDF4Source = HDF4Source( data_url  )                              # Creates source encapsulating all matched files in data_url
ds0: xr.Dataset = h4s.read_partition( part_index )                     # Each partition corresponds to a single file, downloads file from s3 to local cache before reading.

print(f"\n HDF4Source DATASET DS0:" )
print( f"\n ***  attributes:")
for vid, v in ds0.attrs.items():
    print(f" ----> {vid}: {v}")
print( f"\n ***  variables:")
for vid, v in ds0.variables.items():
    print(f" ----> {vid}{v.dims} ({v.shape})")

# stores = h4s.export( location=f"file:/{output_dir}" )    # Exports all partitions, Zarr is the default export format

store = h4s.export( f"file:/{output_file}" )

print( f"Exported file '{ds0.attrs['remote_file']}' (cached at '{ds0.attrs['local_file']}') to '{store}'")
print( "Sample catalog entry:" )
print( ZarrSource( store ).yaml() )