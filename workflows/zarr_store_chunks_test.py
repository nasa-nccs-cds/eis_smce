import os, sys, logging, xarray as xa
import numpy as np
from dask.array.core import Array
from dask.utils import IndexCallable

logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

zarr_store   = "/gpfsm/dnb43/projects/p151/zarr/LIS/DELTA_2km/SCENARIO_2/ROUTING/LIS_HIST.d01.zarr"
varname = "FloodedArea_tavg"

zds: xa.Dataset = xa.open_zarr( zarr_store )
xvar: xa.DataArray = zds.data_vars[varname]
daskvar: Array = xvar.data
print( f"Variable Shape: {daskvar.shape}" )
print( f"Chunk sizes: {daskvar.chunksize}" )
print( f"Chunk Dims: {daskvar.numblocks}" )
print( f"Timeslice size: {np.prod(daskvar.shape[1:])}" )

def show_nan_ts( variable: np.ndarray, ix, iy  ):
    subvar: np.ndarray = variable[:,iy,ix].squeeze()
    nan_dist: np.ndarray = np.isnan(subvar)
    nan_ts = np.full( nan_dist.shape, ".", dtype=np.str_ )
    nan_ts[nan_dist] = "N"
    print( ''.join( nan_ts.tolist() ) )

def show_nan_ts( variable: np.ndarray, ix, iy  ):
    subvar: np.ndarray = variable[:,iy,ix].squeeze()
    print( ''.join( subvar.tolist() ) )

def map_nan_dist( variable: np.ndarray ):
    nt = variable.shape[0]
    subvar: np.ndarray = variable[:,:240,:240]
    nan_dist: np.ndarray = np.count_nonzero(np.isnan(subvar), axis=0).squeeze()
    nan_dist_char = ((nan_dist % 93)+33).astype(np.uint8).view('S1')
    nan_mask = (nan_dist == nt)
    valid_mask = (nan_dist == 0)
    undef_mask = ~(nan_mask | valid_mask)
    nan_dist_map = np.full( nan_dist.shape, ".", dtype=np.str_ )
    nan_dist_map[valid_mask] = "_"
    result: np.ndarray =  np.where( undef_mask, nan_dist_char, nan_dist_map )
    for iL in range( result.shape[0] ):
        print( ''.join( result[iL].tolist() ) )

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


show_nan_ts( xvar.values, 200, 220 )
show_nan_ts( xvar.values, 200, 220 )
map_nan_dist( xvar.values )




