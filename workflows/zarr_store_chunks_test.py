import os, sys, logging, xarray as xa
import numpy as np
from dask.array.core import Array
from dask.utils import IndexCallable

logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

zarr_store = "/gpfsm/dnb43/projects/p151/zarr/LIS/DELTA_2km/SCENARIO_2/ROUTING/LIS_HIST.d01.zarr"
varname = "FloodedArea_tavg"

zds: xa.Dataset = xa.open_zarr( zarr_store )
variable: Array = zds.data_vars[varname].data
print( f"Chunk Dims: {variable.numblocks}" )

i0 = 0
for i1 in range( variable.numblocks[1] ):
    for i2 in range(variable.numblocks[2]):
        chunk: np.ndarray = variable.blocks[i0,i1,i2].compute()
        num_nan = np.count_nonzero(np.isnan(chunk))
        print( f"Chunk[{i0},{i1},{i2}]: #NaN: {num_nan}/{chunk.size}" )

#
#
# for iC, chunks in enumerate(variable.chunks):
#     for ic, chunk in enumerate(chunks):
#         print( f"C{iC}[{ic}]: {chunk}" )
#         if iC == 2: break
#
