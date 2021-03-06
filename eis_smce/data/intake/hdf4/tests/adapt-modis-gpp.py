
import os, intake, xarray as xr
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

if __name__ == '__main__':
    h4s: HDF4Source = intake.open_hdf4( data_url  )                              # Creates source encapsulating all matched files in data_url
    h4s.export( output_file, cache_overwrite=False )
    zs = ZarrSource( output_file )

    print( "\nZarrSource:" )
    dset: xr.Dataset = zs.to_dask()
    print( dset )

    print( "\n --> Dataset Attributes:" )
    for k,v in dset.attrs.items():
        print( f"   ... {k} = {v}" )

