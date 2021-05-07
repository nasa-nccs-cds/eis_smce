import os, xarray as xa
import numpy as np

time_index = 100
vname = "GPP_tavg"
zarr_dest0 = "/gpfsm/dnb43/projects/p151/zarr/LIS/OL_10km/1980/MERRA_IMERG.zarr"
zarr_dest1 = "/gpfsm/dnb43/projects/p151/zarr/LIS/OL_10km/1980/MERRA_IMERG.zarr_Swnet_tavg-SWdown_f_tavg-LWdown_f_tavg-Lwnet_tavg"
zarr_dest2 = "/discover/nobackup/tpmaxwel/cache/zarr_test.zarr"

zds: xa.Dataset = xa.open_zarr( zarr_dest2 )
sample_input = zds['_eis_source_path'].values[time_index]
print( f"sample input path: {sample_input}"  )
ids: xa.Dataset = xa.open_dataset( sample_input )

print( f"\nids attrs:"  )
for k,v in ids.attrs.items():
    print( f"  **  {k}: {v}")

print( f"\nzds attrs:"  )
for k,v in zds.attrs.items():
    print( f"  **  {k}: {v}")

#tvals = zds['time'].values
#fvals = zds['_eis_source_path'].values
for iT in range(100):
#    print( f" {iT}: {os.path.basename(zds['_eis_source_path'].values[iT])}")
    print( f" {iT}: {os.path.basename(zds['time'].values[iT])}")

with xa.set_options( display_max_rows=100 ):
    print( f"\nzds:"  )
    print( zds )

    test_array: xa.DataArray = zds[vname]
    print( f"\n{vname} shape: {test_array.shape}"  )
    print( f"{vname} attrs: {test_array.attrs}"  )
    test_data: np.ndarray = test_array[time_index].max().values
    print( f"{vname} max:"  )
    print( test_data )

    print( f"\nids:"  )
    print( ids )
    input_array: xa.DataArray = ids[vname]
    print( f"\n{vname} input_array shape: {input_array.shape}"  )
    print( f"{vname} input_array attrs: {input_array.attrs}"  )
    input_data: np.ndarray = input_array.max( ).values
    print( f"{vname} max:"  )
    print( input_data )

