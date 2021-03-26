import rioxarray as rxr
from xarray.core.dataset import Dataset
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable

cache_file = "/home/jovyan/.eis_smce/cache/MOD14.A2020298.1835.061.2020348153757.hdf"
modis_pre: List[Dataset] = rxr.open_rasterio( cache_file )

for dset in modis_pre:
    print( f"{ dset.items() }" )