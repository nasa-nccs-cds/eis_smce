from xarray.core.dataset import Dataset
from xarray.core.variable import Variable
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable

cache_file = "/home/jovyan/.eis_smce/cache/MOD14.{sample}.hdf"
h4s: HDF4Source = HDF4Source( cache_file )
ds0: Dataset = h4s.read_partition(0)

print(f"\n HDF4Source DATASET DS0:" )
print( f" *** attributes:\n {ds0.attrs}")
print( f" *** variables:\n")
for vid, v in ds0.variables.items():
    print(f" ----> {vid}{v.dims} ({v.shape})")



