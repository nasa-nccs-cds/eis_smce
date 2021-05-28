from xarray.core.dataset import Dataset
from xarray.core.variable import Variable
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable

data_url = 's3://eis-dh-fire/mod14/raw/MOD14.A2020299.*.hdf'
h4s: HDF4Source = HDF4Source( data_url )

ds0 = h4s.read_partition( 0 )
print( f"\n *** ds0 variables:")
for vid, v in ds0.variables.items():
    print(f" ----> {vid}{v.dims} ({v.shape})")

ds1 = h4s.read_partition( 1 )
print( f"\n *** ds1 variables:")
for vid, v in ds1.variables.items():
    print(f" ----> {vid}{v.dims} ({v.shape})")

dsm: Dataset = h4s.read()
print( f"\n *** Merged variables:")
for vid, v in dsm.variables.items():
    print(f" ----> {vid}{v.dims} ({v.shape})")


