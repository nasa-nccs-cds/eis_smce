import os, sys, logging, time, xarray as xa
import numpy as np

logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

if len(sys.argv) == 1:
    print( f"Usage: >> python {sys.argv[0]} <config_file_path>")
    sys.exit(-1)

zarr_dset = sys.argv[1]
time_index = 2

zds: xa.Dataset = xa.open_zarr( zarr_dset )
with xa.set_options( display_max_rows=100 ):
    print( f"\nzds:"  )
    print( zds )

    vname = list(zds.variables.keys())[0]
    t0 = time.time()
    test_array: xa.DataArray = zds[vname]
    print( f"\n{vname} shape: {test_array.shape}, dims = {test_array.dims}"  )
    print(f" --> {vname} chunks: {test_array.chunks}")
    print(f" --> {vname} attrs: {test_array.attrs}"  )
    test_data: np.ndarray = test_array[time_index].max().values
    print( f" -> {vname}[{time_index}], max: {test_data}: Computed in {time.time()-t0} sec."  )