import os, traitlets.config as tlc
from eis_smce.data.common.base import EISSingleton
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import intake, time, traceback, dask
import xarray as xa
from eis_smce.data.intake.zarr.source import EISZarrSource

class ZarrConverter(EISSingleton):

    def __init__(self):
        super(ZarrConverter, self).__init__()


    def standard_conversion(self, input: str, output: str, **kwargs ) -> EISZarrSource:
        try:
            t0 = time.time()
            h4s = intake.open_hdf4( input )
            h4s.export_parallel( output, **kwargs )
            print( f"Completed conversion & upload to {output} in {(time.time()-t0)/60} minutes" )
        except Exception as err:
            self.logger.error( f"Error in ZarrConverter.standard_conversion: {err}")
            self.logger.error(f"{traceback.format_exc()}")

    def get_input(self, input: str, **kwargs ) -> xa.Dataset:
        h4s = intake.open_hdf4(input)
        return h4s.to_dask(**kwargs)

    def standard_conversions( self, dsets: List[Dict[str,str]], **kwargs ):
        for dset in dsets:
            [input, output] = [ dset.pop(key) for key in ['input', 'output'] ]
            zc().standard_conversion( input, output, **kwargs, **dset )

def zc(): return ZarrConverter.instance()
