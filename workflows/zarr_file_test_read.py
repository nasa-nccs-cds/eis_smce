import os, sys, logging, xarray as xa
import numpy as np

logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

if len(sys.argv) == 1:
    print( f"Usage: >> python {sys.argv[0]} <config_file_path>")
    sys.exit(-1)

zarr_dset = sys.argv[1]
time_index = 2

zds: xa.Dataset = xa.open_zarr( zarr_dset )
sample_input = zds['_eis_source_path'].values[time_index]
print( f"sample input path: {sample_input}"  )
ids: xa.Dataset = xa.open_dataset( sample_input )

print( f"\nids attrs:"  )
for k,v in ids.attrs.items():
    print( f"  **  {k}: {v}")

print( f"\nzds attrs:"  )
for k,v in zds.attrs.items():
    print( f"  **  {k}: {v}")

tvals = zds['time'].values
fvals = zds['_eis_source_path'].values
for iT in range(0,time_index):
    print( f" {zds['time'].values[iT]}: {os.path.basename(zds['_eis_source_path'].values[iT])}")

with xa.set_options( display_max_rows=100 ):
    print( f"\nzds:"  )
    print( zds )

    vname = list(zds.variables.keys())[0]
    test_array: xa.DataArray = zds[vname]
    print( f"\n{vname} shape: {test_array.shape}"  )
    print(f" --> {vname} chunks: {test_array.chunks}")
    print(f" --> {vname} attrs: {test_array.attrs}"  )
    test_data: np.ndarray = test_array[time_index].max().values
    print( f" -> {vname} max:"  )
    print( test_data )

    print( f"\nids:"  )
    print( ids )
    input_array: xa.DataArray = ids[vname]
    print( f"\n{vname} input_array shape: {input_array.shape}"  )
    print( f"{vname} input_array attrs: {input_array.attrs}"  )
    input_data: np.ndarray = input_array.max( ).values
    print( f"{vname} max:"  )
    print( input_data )

