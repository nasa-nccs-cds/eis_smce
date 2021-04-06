import os, xarray as xr
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from intake_xarray.xzarr import ZarrSource

base_dir = "/css/modis/Collection6/L3/"
cache_dir = "/att/nobackup/tpmaxwel/ILAB/scratch"
collection = "MOD17A2H-GPP"
year = "2004"
day = "0*"
file_names = "MOD17A2H.{sample}.h09v09.006.{sid}.hdf"
batch = f"{year}/{day}/{file_names}"
output_file = f"{cache_dir}/{collection}/h09v09.zarr"
os.makedirs( os.path.dirname(output_file), exist_ok=True )

data_url = f"file://{base_dir}/{collection}/{batch}"
h4s: HDF4Source = HDF4Source( data_url  )                              # Creates source encapsulating all matched files in data_url
h4s.export( output_file )
zs = ZarrSource( output_file )

print( "\nZarrSource:" )
dset: xr.Dataset = zs.to_dask()
print( dset )

print( "\n --> Dataset Attributes:" )
for k,v in dset.attrs.items():
    print( f"   ... {k} = {v}" )


