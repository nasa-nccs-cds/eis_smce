import rioxarray as rxr
import rasterio as rio
from xarray.core.dataset import Dataset
from xarray.core.variable import Variable
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable

# cache_file = "/home/jovyan/.eis_smce/cache/MOD14.A2020298.1835.061.2020348153757.hdf"
cache_file = "/Users/tpmaxwel/Dropbox/HDF_file/MOD05_L2.A2021080.0000.061.NRT.hdf"
modis_pre: List[Dataset] = rxr.open_rasterio( cache_file )

for ids in range( len(modis_pre) ):
    ds0: Dataset = modis_pre[ids]
    print(f"\n\n DATASET DS-{ids}:" )
    print( f" *** attributes:\n {ds0.attrs}")
    print( f" *** variables:\n")
    for vid, v in ds0.variables.items():
        print(f" ----> {vid}{v.dims} ({v.shape})")



