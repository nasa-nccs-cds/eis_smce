import xarray as xa
import numpy as np

zarr_dest = "/gpfsm/dnb43/projects/p151/zarr/freshwater.swang.2013/output/ROUTING/LIS_RST_HYMAP2_router.d01.zarr"

dset: xa.Dataset = xa.open_zarr( zarr_dest )

print( dset )

RNFSTO: xa.DataArray = dset['RNFSTO']

print( f"RNFSTO shape: {RNFSTO.shape}"  )

test_data: np.ndarray = RNFSTO.max( axis = 1 )
print( f"RNFSTO max (axis=1):"  )
print( test_data )