import numpy as np
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import os, glob, xarray as xr

batch = os.path.expanduser(f"~/.eis_smce/cache/NACRSULL/*.nc")
data_files = glob.glob( batch )

test_var = "QC"
file_indices = range( len(data_files) )

df0: xr.Dataset = data_files[0]

print( f"DIMS: {df0.dims}" )
print( f"COORDS: {list(df0.coords.keys())}" )
print( f"ITEMS: {list(df0.keys())}" )
print( f"VARS: {list(df0.variables.keys())}" )

exit(0)

def preprocess_for_merge( ds: xr.Dataset ) -> xr.Dataset:
    pass

for iF in file_indices:
    print( f"Processing data file: {data_files[iF]}" )
    single_dataset: xr.Dataset = xr.open_dataset( data_files[iF] )
    unmerged_data_array: xr.DataArray = single_dataset[test_var].expand_dims( {"sample":np.array([iF])}, 0 )
    print( f'\n\nunmerged_data_array[{iF}:' )
    print( unmerged_data_array )

merge_index = xr.IndexVariable( 'sample', np.array(file_indices) )
merged_dataset: xr.Dataset = xr.open_mfdataset( data_files, concat_dim='sample' )   # preprocess to expand_dims:  preprocess=
merged_data_array: xr.DataArray = merged_dataset[test_var]
print( '\n\nmerged_data_array:' )
print( merged_data_array )