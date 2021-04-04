import os, xarray as xr
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from intake_xarray.xzarr import ZarrSource

use_batch = True
data_file_url = "file:///Users/tpmaxwel/allData/61/MCDWD_L3_NRT/2020/292/MCDWD_L3_NRT.A2020292.h00v02.061.hdf"
data_batch_url = "file:///Users/tpmaxwel/allData/61/MCDWD_L3_NRT/2020/292/MCDWD_L3_NRT.A2020292.{hv}.061.hdf"
part_index: int = 0
data_intput = data_batch_url if use_batch else data_file_url


h4s: HDF4Source = HDF4Source( data_batch_url  )                        # Creates source encapsulating all matched files in data_url
ds0: xr.Dataset = h4s.read_partition( part_index )                    # Each partition corresponds to a single file, downloads file from s3 to local cache before reading.

remote_input_file: str = ds0.attrs['remote_file']
local_input_file: str  = ds0.attrs['local_file']
remote_zarr_file: str = "file:///Users/tpmaxwel/allData/61/MCDWD_L3_NRT/2020/292/MCDWD_L3_NRT.A2020292.061.zarr"

print(f"\n HDF4Source DATASET DS0:" )
print( f"\n ***  attributes:")
for vid, v in ds0.attrs.items():
    print(f" ----> {vid}: {v}")
print( f"\n ***  variables:")
for vid, v in ds0.variables.items():
    print(f" ----> {vid}{v.dims} ({v.shape})")

xzSources: List[ZarrSource] = h4s.export( remote_zarr_file )                   # Exports the current partition (index = 0), Zarr is the default export format
print( f"Exported file '{remote_input_file}' (cached at '{local_input_file}') to '{remote_zarr_file}'")
zsc: ZarrSource = xzSources[0]
data: xr.Dataset = zsc.read()
print( "\n\nZarr Data:" )
print( data )



