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

    def get_plots(self) -> Dict:
        return dict( contour = dict( kind='contourf', groupby='sample', width = 800, height=600, levels=20 ) )

    def yaml(self, **kwargs) -> str:
        dset: xr.Dataset = self.to_dask()
        description = self.get_attribute(dset, kwargs.get('description', 'att:LONGNAME'))
        self.cat_name = self.get_attribute(dset, kwargs.get('name', 'att:SHORTNAME'), self.urlpath.split("/")[-1])
        metadata = { **dset.attrs }
        metadata.update( kwargs.get("metadata", {} ) )
        metadata['dimensions'] = {key: list(c.shape) for key, c in dset.coords.items()}
        metadata['variables'] = {key: list(v.dims) for key, v in dset.items()}
        metadata['plots'] = self.get_plots()
        data = { self.cat_name: { 'driver': self.classname, 'description': description, 'metadata': metadata, 'args': dict( urlpath=self.urlpath ) } }
        return yaml.dump( data, indent=2 )