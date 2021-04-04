import numpy as np
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import os, glob, xarray as xr

base_dir = "/att/nobackup/mcarrol2/MODIS"
cache_dir = "/att/nobackup/tpmaxwel/ILAB/scratch"
collection = "MCD12Q1"
location = "h09v09"
batch = f"200?/001/MCD12Q1.A200?001.{location}.006.*.hdf"
output_file = f"{cache_dir}/{collection}/{location}.zarr"
os.makedirs( os.path.dirname(output_file), exist_ok=True )
data_files = glob.glob( f"{base_dir}/{collection}/{batch}" )

test_var = "QC"
file_indices = range( len(data_files) )

for iF in file_indices:
    single_dataset: xr.Dataset = xr.open_dataset( data_files[iF] )
    unmerged_data_array: xr.DataArray = single_dataset[test_var].expand_dims( {"sample":np.array([iF])}, 0 )
    print( f'\n\nunmerged_data_array[{iF}:' )
    print( unmerged_data_array )

merge_index = xr.IndexVariable( 'sample', np.array(file_indices) )
merged_dataset: xr.Dataset = xr.open_mfdataset( data_files, concat_dim='sample' )   # preprocess to expand_dims:  preprocess=
merged_data_array: xr.DataArray = merged_dataset[test_var]
print( '\n\nmerged_data_array:' )
print( merged_data_array )