import os, sys, logging, xarray as xa
import numpy as np
from dask.array.core import Array

logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

zarr_store = "/gpfsm/dnb43/projects/p151/zarr/LIS/DELTA_2km/SCENARIO_2/ROUTING/LIS_HIST.d01.zarr"
varname = "FloodedArea_tavg"

zds: xa.Dataset = xa.open_zarr( zarr_store )
variable: Array = zds.data_vars[varname].persist().data

for iC, chunk in enumerate(variable.chunks):
    print( chunk.__class__ )
    if iC == 10: break

