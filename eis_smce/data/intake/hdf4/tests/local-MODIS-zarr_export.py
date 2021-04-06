import os, xarray as xr
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from intake_xarray.xzarr import ZarrSource

batch = "file:///Users/tpmaxwel/Dropbox/Data/MCD12Q1/*.nc"
cache = "/Users/tpmaxwel/Dropbox/Data/cache"
part_index: int = 0

if __name__ == '__main__':
    h4s: HDF4Source = HDF4Source( batch  )                        # Creates source encapsulating all matched files in data_url
    ds0: xr.Dataset = h4s.read_partition( part_index )             # Each partition corresponds to a single file, downloads file from s3 to local cache before reading.

    remote_input_file: str = ds0.attrs['remote_file']
    local_input_file: str  = ds0.attrs['local_file']
    remote_zarr_file: str = "file:///Users/tpmaxwel/Dropbox/Data/cache/MCD12Q1.zarr"


    xzSources: List[ZarrSource] = h4s.export( remote_zarr_file )                   # Exports the current partition (index = 0), Zarr is the default export format
    print( f"Exported file '{remote_input_file}' (cached at '{local_input_file}') to '{remote_zarr_file}'")
    zsc: ZarrSource = xzSources[0]
    data: xr.Dataset = zsc.read()
    print( "\n\nZarr Data:" )
    print( data )



