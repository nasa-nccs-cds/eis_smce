import os, traitlets.config as tlc
from eis_smce.data.common.base import EISSingleton
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import intake, time, traceback
from eis_smce.data.intake.zarr.source import EISZarrSource
from eis_smce.data.intake.catalog import cm

class ZarrConverter(EISSingleton):

    def standard_conversion(self, input: str, output: str, **kwargs ) -> EISZarrSource:
        try:
            t0 = time.time()
            h4s = intake.open_hdf4( input )
            zs: EISZarrSource = h4s.export( output, **kwargs )
            cm().addEntry( zs  )
            if zs: print( f"Completed {zs.cat_name} conversion & upload to {output} in {(time.time()-t0)/60} minutes" )
            return zs
        except Exception as err:
            self.logger.error( f"Error in ZarrConverter.standard_conversion: {err}")
            self.logger.error(f"{traceback.format_exc()}")


def zc(): return ZarrConverter.instance()
