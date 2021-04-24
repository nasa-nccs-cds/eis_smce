import xarray as xa
import numpy as np

sample_input = "/discover/nobackup/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013/ROUTING/201505/LIS_HIST_201505170000.d01.nc"
zarr_dest = "/gpfsm/dnb43/projects/p151/zarr/freshwater.swang.2013/output/ROUTING/LIS_HIST.d01.zarr"
vname = "FLDSTO"

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

test_array: xa.DataArray = zds[vname]
print( f"\n{vname} shape: {test_array.shape}"  )
print( f"{vname} attrs: {test_array.attrs}"  )
test_data: np.ndarray = test_array.max( axis = 1 ).values
print( f"{vname} max (axis=1):"  )
print( test_data )

print( f"\nids:"  )
print( ids )
input_array: xa.DataArray = ids[vname]
print( f"\n{vname} input_array shape: {input_array.shape}"  )
print( f"{vname} input_array attrs: {input_array.attrs}"  )
input_data: np.ndarray = input_array.max( ).values
print( f"{vname} max:"  )
print( input_data )

