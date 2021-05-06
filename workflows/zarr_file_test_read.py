import xarray as xa
import numpy as np

sample_input = "/discover/nobackup/projects/eis_freshwater/swang9/OL_10km/OUTPUT.1980.imerg.fixed/SURFACEMODEL/200007/LIS_HIST_200007160000.d01.nc"
zarr_dest = "/gpfsm/dnb43/projects/p151/zarr/LIS/OL_10km/1980/2000_2021/MERRA_IMERG.zarr"
vname = "SoilMoist_tavg"

ids: xa.Dataset = xa.open_dataset( sample_input )
zds: xa.Dataset = xa.open_zarr( zarr_dest )

print( f"\nids attrs:"  )
for k,v in ids.attrs.items():
    print( f"  **  {k}: {v}")

print( f"\nzds attrs:"  )
for k,v in zds.attrs.items():
    print( f"  **  {k}: {v}")

print( f"\nzds:"  )
print( zds )
print( f"\nzds['history'] = {zds['_history'].values}" )
print( f"zds['time'] = {zds['_time'].values}" )

test_array: xa.DataArray = zds[vname]
print( f"\n{vname} shape: {test_array.shape}"  )
print( f"{vname} attrs: {test_array.attrs}"  )
axes = list( range( 1, test_array.ndim ) )
test_data: np.ndarray = test_array.max( axis = axes ).values
print( f"{vname} max (axis={axes}):"  )
print( test_data )

print( f"\nids:"  )
print( ids )
input_array: xa.DataArray = ids[vname]
print( f"\n{vname} input_array shape: {input_array.shape}"  )
print( f"{vname} input_array attrs: {input_array.attrs}"  )
input_data: np.ndarray = input_array.max( ).values
print( f"{vname} max:"  )
print( input_data )

