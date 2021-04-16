import os, traitlets.config as tlc
import zarr
from eis_smce.data.common.base import EISSingleton
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable

class ZarrConverter(EISSingleton):

    default_cache_dir = tlc.Unicode( os.path.expanduser("~/.eis_smce/cache") ).tag(config=True)

    def __init__( self, **kwargs ):
        EISSingleton.__init__( self, **kwargs )
        self.cache_dir = kwargs.get('cache', self.default_cache_dir )
        os.makedirs( self.cache_dir, exist_ok=True )

    def standard_conversion(self, destination: str):
