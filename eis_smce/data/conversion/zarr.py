import os, traitlets.config as tlc
from eis_smce.data.common.base import EISSingleton
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import intake, time, traceback, dask
from eis_smce.data.intake.zarr.source import EISZarrSource

class ZarrConverter(EISSingleton):

    def __init__(self):
        super(ZarrConverter, self).__init__()


    def standard_conversion(self, input: str, output: str, **kwargs ) -> EISZarrSource:
        try:
            t0 = time.time()
            h4s = intake.open_hdf4( input )
            zs: EISZarrSource = h4s.export( output, **kwargs )
            if zs: print( f"Completed {zs.cat_name} conversion & upload to {output} in {(time.time()-t0)/60} minutes" )
            return zs
        except Exception as err:
            self.logger.error( f"Error in ZarrConverter.standard_conversion: {err}")
            self.logger.error(f"{traceback.format_exc()}")

    def standard_conversions( self, dsets: List[Dict[str,str]], **kwargs ) -> List[EISZarrSource]:
        parallel = False
        sources = []
        for dset in dsets:
            [input, output] = [ dset.pop(key) for key in ['input', 'output'] ]
            convert = dask.delayed( zc().standard_conversion ) if parallel else zc().standard_conversion
            sources.append( convert( input, output, **kwargs, **dset ) )
        return dask.compute( sources ) if parallel else sources


def zc(): return ZarrConverter.instance()
