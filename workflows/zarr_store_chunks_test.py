import os, sys, logging, xarray as xa
import numpy as np

logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

if len(sys.argv) == 1:
    print( f"Usage: >> python {sys.argv[0]} <zarr_store_path>")
    sys.exit(-1)

zarr_dset = sys.argv[1]

zds: xa.Dataset = xa.open_zarr( zarr_dset )

print( list( zds.data_vars.keys() ) )