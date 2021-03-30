from xarray.core.dataset import Dataset
from xarray.core.variable import Variable
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable

part_index = 0
data_url = "s3://eis-dh-fire/mod14/raw/MOD14.{sample}.hdf"
zarr_url = "s3://eis-dh-fire/mod14/raw/MOD14.{sample}.zarr"

h4s: HDF4Source = HDF4Source( data_url  )
ds0: Dataset = h4s.read_partition( part_index )

print(f"\n HDF4Source DATASET DS0:" )
print( f"\n ***  attributes:")
for vid, v in ds0.attrs.items():
    print(f" ----> {vid}: {v}")
print( f"\n ***  variables:")
for vid, v in ds0.variables.items():
    print(f" ----> {vid}{v.dims} ({v.shape})")

exSoruce = h4s.export( zarr_url )
print( f"Exported file {part_index} from {data_url} to ")



