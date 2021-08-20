from eis_smce.data.common.base import EISSingleton
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from eis_smce.data.intake.source import EISDataSource
import yaml, intake, time, traceback, dask
import xarray as xa

class ZarrConverter(EISSingleton):

    def __init__(self):
        super(ZarrConverter, self).__init__()
        self.start_time = None

    def standard_conversion(self, input: str, output: str, **kwargs ):
        try:
            self.start_time =   time.time()
            eisds: EISDataSource = EISDataSource( input, **kwargs )
            eisds.export( output,**kwargs )
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

    def get_attribute(self, dset: xa.Dataset, attval: str, default: str = ""):
        if attval.startswith('att:'):
            return dset.attrs.get(attval[4:], default)
        else:
            return attval

    def write_catalog( self, zpath: str, cat_name: str, **kwargs ):
        cat_path = self._cat_path(cat_name)
        print(f"Writing catalog for zarr dset at {zpath} to {cat_path}")
        dset: xa.Dataset = xa.open_zarr( zpath )
        description = self.get_attribute(dset, kwargs.pop('description', 'att:LONGNAME'))
        metadata = {**dset.attrs}
        metadata.update(kwargs.pop("metadata", {}))
        metadata['dimensions'] = {key: list(c.shape) for key, c in dset.coords.items()}
        metadata['variables'] = {key: list(v.dims) for key, v in dset.items()}
        data = { cat_name: {'driver': "ZarrSource", 'description': description, 'metadata': metadata, 'args': dict(urlpath=zpath)}}
        cat_file = open( cat_path, "w")
        cat_file.write( yaml.dump(data, indent=2) )

def zc(): return ZarrConverter.instance()
