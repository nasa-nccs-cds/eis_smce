import os, sys, logging, xarray as xa
import numpy as np

logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

zarr_store = "/gpfsm/dnb43/projects/p151/zarr/LIS/DELTA_2km/SCENARIO_2/ROUTING/LIS_HIST.d01.zarr"
varname = "FloodedArea_tavg"

zds: xa.Dataset = xa.open_zarr( zarr_store )
variable: xa.DataArray = zds.data_vars[varname]
vslice: xa.DataArray = variable[0:25]
print( vslice )

def test_chunk( chunk: xa.DataArray ) -> xa.DataArray:
    print( f"{chunk.shape}" )
    return chunk

result = xa.map_blocks( test_chunk, variable )