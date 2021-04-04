import os, xarray as xr
import numpy as np
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable

data_files = [ "/Users/tpmaxwel/allData/61/MCDWD_L3_NRT/2020/292/MCDWD_L3_NRT.A2020292.h00v02.061.nc", "/Users/tpmaxwel/allData/61/MCDWD_L3_NRT/2020/292/MCDWD_L3_NRT.A2020292.h26v06.061.nc" ]
test_var = "Water Counts 3-Day 250m"
file_indices = range( len(data_files) )

for iF in file_indices:
    single_dataset: xr.Dataset = xr.open_dataset( data_files[iF] )
    unmerged_data_array: xr.DataArray = single_dataset[test_var].expand_dims( {"sample":np.array([iF])}, 0 )
    print( f'\n\nunmerged_data_array[{iF}:' )
    print( unmerged_data_array )

merge_index = xr.IndexVariable( 'sample', np.array(file_indices) )
merged_dataset: xr.Dataset = xr.open_mfdataset( data_files, concat_dim='sample', preprocess= )   # preprocess to expand_dims
merged_data_array: xr.DataArray = merged_dataset[test_var]
print( '\n\nmerged_data_array:' )
print( merged_data_array )