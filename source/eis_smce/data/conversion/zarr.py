from eis_smce.data.common.base import EISSingleton
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from eis_smce.data.intake.source import EISDataSource
import intake, time, traceback, dask
import xarray as xa

class ZarrConverter(EISSingleton):

    def __init__(self):
        super(ZarrConverter, self).__init__()
        self.start_time = None

    def standard_conversion(self, input: str, output: str ):
        try:
            self.start_time = time.time()
            eisds: EISDataSource = EISDataSource( input )
            eisds.export_parallel( output )
            print( f"Completed conversion in {(time.time()-self.start_time)/60} minutes" )
        except Exception as err:
            self.logger.error( f"Error in ZarrConverter.standard_conversion: {err}")
            self.logger.error(f"{traceback.format_exc()}")
            traceback.print_exc()

    def get_input(self, input: str, **kwargs ) -> xa.Dataset:
        h4s = intake.open_hdf4(input)
        return h4s.to_dask(**kwargs)

    def _cat_path( self, cat_name: str ) -> str:
        from eis_smce.data.common.base import eisc
        return f"{eisc().cat_dir}/{cat_name}.yml"

    def write_catalog( self, zpath: str, cat_name: str, **kwargs ):
        from eis_smce.data.intake.zarr.source import EISZarrSource
        catalog_args = { k: kwargs.pop(k,None) for k in [ 'discription', 'name', 'metadata'] }
        zsrc = EISZarrSource( zpath, **kwargs )
        cat_path = self._cat_path(cat_name)
        cat_file = open( cat_path, "w")
        print( f"Writing catalog to {cat_path}")
        cat_file.write( zsrc.yaml( **catalog_args ) )

def zc(): return ZarrConverter.instance()
