from intake_xarray.xzarr import ZarrSource
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import yaml, xarray as xr


class EISZarrSource( ZarrSource ):

    def __init__(self, urlpath, storage_options=None, metadata=None, **kwargs):
        ZarrSource.__init__( self, urlpath, storage_options, metadata, **kwargs )
        self.cat_name = None

    def get_attribute(self, dset: xr.Dataset, attval: str, default: str = "" ):
        if attval.startswith('att:'):  return dset.attrs.get( attval[4:], default )
        else:                          return attval

    def yaml(self, **kwargs) -> str:
        dset: xr.Dataset = self.to_dask()
        description = self.get_attribute(dset, kwargs.get('description', 'att:LONGNAME'))
        self.cat_name = self.get_attribute(dset, kwargs.get('name', 'att:SHORTNAME'), self.urlpath.split("/")[-1])
        metadata = { **dset.attrs }
        metadata.update( kwargs.get("metadata", {} ) )
        data = {
            'sources':
                {self.cat_name: {
                    'driver': self.classname,
                    'description': description,
                    'dimensions': {key: list(c.shape) for key, c in dset.coords.items()},
                    'variables': {key: list(v.dims) for key, v in dset.items()},
                    'metadata': metadata,
                }}}
        return yaml.dump(data, default_flow_style=False)