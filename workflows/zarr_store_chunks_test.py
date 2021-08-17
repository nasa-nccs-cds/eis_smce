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
print( f"Variable Shape: {variable.shape}" )
print( f"Chunk sizes: {variable.chunksize}" )
print( f"Chunk Dims: {variable.numblocks}" )
print( f"Timeslice size: {np.prod(variable.shape[1:])}" )

i0 = 0
for i1 in range( variable.numblocks[1] ):
    for i2 in range(variable.numblocks[2]):
        chunk: np.ndarray = variable.blocks[i0,i1,i2].compute()
        num_nan = np.count_nonzero(np.isnan(chunk))
        print( f"Chunk[{i0},{i1},{i2}]: shape={chunk.shape}, size = {chunk.size}, #NaN: {num_nan}" )

print("\n")
(i0,i1,i2) = ( 0, 2, 3 )
chunk: np.ndarray = variable.blocks[ i0, i1, i2 ].compute()
for iS in range( chunk.shape[0] ):
    cslice = chunk[iS]
    num_nan = np.count_nonzero(np.isnan(cslice))
    print(f"Chunk[{i0},{i1},{i2}][{iS}]: shape={cslice.shape}, size = {cslice.size}, #NaN: {num_nan}")

missing_blocks = []
for i0 in range( variable.numblocks[0] ):
    for i1 in range( variable.numblocks[1] ):
        for i2 in range(variable.numblocks[2]):
            chunk: np.ndarray = variable.blocks[i0,i1,i2].compute()
            if np.count_nonzero(~np.isnan(chunk)) == 0:
                missing_blocks.append( (i0,i1,i2) )

print(f"\nMissing Blocks: {missing_blocks}")
