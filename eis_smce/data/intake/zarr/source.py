from intake_xarray.xzarr import ZarrSource
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import yaml, xarray as xr

class EISZarrSource( ZarrSource ):

    def __init__(self, urlpath, **kwargs ):
        storage_options = kwargs.get( 'storage_options', None )
        metadata = kwargs.get( 'metadata', None )
        ZarrSource.__init__( self, urlpath, storage_options, metadata, **kwargs )

    def get_plots(self) -> Dict:
        return dict( contour = dict( kind='contourf', groupby='sample', width = 800, height=600, levels=20 ) )

    @property
    def zarr_file_path(self):
        zfpath = self.urlpath.split(":")[1].replace("//","/") if self.urlpath.startswith("file:") else self.urlpath
        return f"{zfpath}.zarr"

    def get_attribute(self, dset: xr.Dataset, attval: str, default: str = "" ):
        if attval.startswith('att:'):  return dset.attrs.get( attval[4:], default )
        else:                          return attval

    def yaml(self, **kwargs) -> str:
        print( f"Generating yaml for zarr file at {self.zarr_file_path }")
        dset: xr.Dataset = xr.open_zarr( self.zarr_file_path )
        description = self.get_attribute(dset, kwargs.get('description', 'att:LONGNAME'))
        self.cat_name = self.get_attribute(dset, kwargs.get('name', 'att:SHORTNAME'), self.urlpath.split("/")[-1])
        metadata = { **dset.attrs }
        metadata.update( kwargs.get("metadata", {} ) )
        metadata['dimensions'] = {key: list(c.shape) for key, c in dset.coords.items()}
        metadata['variables'] = {key: list(v.dims) for key, v in dset.items()}
        metadata['plots'] = self.get_plots()
        data = { self.cat_name: { 'driver': self.classname, 'description': description, 'metadata': metadata, 'args': dict( urlpath=self.urlpath ) } }
        return yaml.dump( data, indent=2 )