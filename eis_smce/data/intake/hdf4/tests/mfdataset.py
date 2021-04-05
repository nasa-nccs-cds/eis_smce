import numpy as np
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import os, glob, xarray as xr

batch = os.path.expanduser(f"~/.eis_smce/cache/NACRSULL/*.nc")
data_files = glob.glob( batch )

test_var = "QC"
file_indices = range( len(data_files) )

df0: xr.Dataset = xr.open_dataset( data_files[0] )

print( f"DIMS: {df0.dims}" )
print( f"COORDS: {list(df0.coords.keys())}" )
print( f"ITEMS: {list(df0.keys())}" )
print( f"VARS: {list(df0.variables.keys())}" )

def preprocess( ds: xr.Dataset ):
    new_vars = {}
    for name, xar in ds.items():
        new_vars[ name ] = xar.expand_dims( {"sample":np.array([ds.attrs['sample']])}, 0 )
    return xr.Dataset( new_vars )

for iF in file_indices:
    print( f"Processing data file: {data_files[iF]}" )
    single_dataset: xr.Dataset = xr.open_dataset( data_files[iF] )
    single_dataset.attrs['sample'] = str(iF)
    single_dataset.to_netcdf( data_files[iF], "w" )

merge_index = xr.IndexVariable( 'sample', np.array(file_indices) )
merged_dataset: xr.Dataset = xr.open_mfdataset( data_files, concat_dim='sample', preprocess=preprocess )
merged_data_array: xr.DataArray = merged_dataset[test_var]
print( '\n\nmerged_data_array:' )
print( merged_data_array )