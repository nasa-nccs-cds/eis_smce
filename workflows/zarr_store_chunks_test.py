import os, sys, logging, xarray as xa
import numpy as np
from dask.array.core import Array
from dask.utils import IndexCallable

logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

zarr_store = "/gpfsm/dnb43/projects/p151/zarr/LIS/DELTA_2km/SCENARIO_2/ROUTING/LIS_HIST.d01.zarr"
varname = "FloodedArea_tavg"

zds: xa.Dataset = xa.open_zarr( zarr_store )
xvar: xa.DataArray = zds.data_vars[varname]
daskvar: Array = xvar.data
print( f"Variable Shape: {daskvar.shape}" )
print( f"Chunk sizes: {daskvar.chunksize}" )
print( f"Chunk Dims: {daskvar.numblocks}" )
print( f"Timeslice size: {np.prod(daskvar.shape[1:])}" )

def map_nan_dist( variable: np.ndarray ):
    nt = variable.shape[0]
    nan_dist: np.ndarray = np.count_nonzero(np.isnan(variable), axis=0).squeeze()
    nan_mask = (nan_dist == nt)
    valid_mask = (nan_dist == 0)
    undef_mask = ~(nan_mask | valid_mask)
    nan_dist_map = np.full( nan_dist.shape, "x", dtype=np.str_ )
    nan_dist_map[valid_mask] = "."
    nan_dist_map[undef_mask] = "*"
    print(nan_dist_map.shape)
    print( nan_dist_map[0] )
    for iL in range( nan_dist.shape[0] ):
        print( ''.join( nan_dist_map[iL].tolist() ) )

def count_missing_blocks( variable: Array ):
    missing_blocks = []
    for i0 in range( variable.numblocks[0] ):
        for i1 in range( variable.numblocks[1] ):
            for i2 in range(variable.numblocks[2]):
                chunk: np.ndarray = variable.blocks[i0,i1,i2].compute()
                if not (np.count_nonzero(~np.isnan(chunk)) > 0):
                    missing_blocks.append( (i0,i1,i2) )
    print(f"\nMissing Blocks: {missing_blocks}")

def explore_blocks(  variable: Array  ):
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


map_nan_dist( xvar.values )




