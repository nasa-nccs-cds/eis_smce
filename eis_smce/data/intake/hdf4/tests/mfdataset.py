import numpy as np
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import os, glob, xarray as xr

batch = "/Users/tpmaxwel/Dropbox/Data/MCD12Q1/*.nc"
cache = "/Users/tpmaxwel/Dropbox/Data/cache"
data_files = glob.glob( batch )

test_var = "QC"
file_indices = range( len(data_files) )

def preprocess( ds: xr.Dataset ):
    new_vars = {}
    for name, xar in ds.items():
        new_vars[ name ] = xar.expand_dims( {"sample":np.array([ds.attrs['sample']])}, 0 )
    return xr.Dataset( new_vars )

new_files = []
for iF in file_indices:
    print( f"Processing data file: {data_files[iF]}" )
    file_name = os.path.basename( data_files[iF] )
    single_dataset: xr.Dataset = xr.open_dataset( data_files[iF] )
    if 'sample' not in list(single_dataset.attrs.keys()): single_dataset.attrs['sample'] = str(iF)
    outfile = f"{cache}/{file_name}"
    single_dataset.to_netcdf( outfile, "w" )
    new_files.append( outfile )

merged_dataset: xr.Dataset = xr.open_mfdataset( new_files, concat_dim='sample', preprocess=preprocess )
merged_data_array: xr.DataArray = merged_dataset[test_var]
print( '\n\nmerged_data_array:' )
print( merged_data_array )