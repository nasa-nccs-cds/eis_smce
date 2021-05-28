from eis_smce.data.common.base import EISSingleton
import zarr
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable

class DataConversionManager(EISSingleton):

    def __init__( self, **kwargs ):
        EISSingleton.__init__( self, **kwargs )

    def load_group(self, group: str, dsets: List[Dict[str,str]] ):
        zgroup = zarr.group( group )
        for dset_spec in dsets:
            source = HDF4Source( )
