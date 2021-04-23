import xarray as xa
import numpy as np

sample_input = "/discover/nobackup/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013/SURFACEMODEL/201402/LIS_HIST_201402080000.d01.nc"
zarr_dest = "/gpfsm/dnb43/projects/p151/zarr/freshwater.swang.2013/output/ROUTING/LIS_RST_HYMAP2_router.d01.zarr"
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

RNFSTO: xa.DataArray = zds[vname]
print( f"\n{vname} shape: {RNFSTO.shape}"  )

print( f"{vname} attrs: {RNFSTO.attrs}"  )

test_data: np.ndarray = RNFSTO.max( axis = 1 ).values
print( f"{vname} max (axis=1):"  )
print( test_data )