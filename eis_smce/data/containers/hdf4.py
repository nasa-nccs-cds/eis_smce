import traitlets.config as tlc
from pyhdf.SD import SD, SDC, SDS
from collections.abc import MutableMapping
from pathlib import Path
import xarray as xa
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable

class ModisDataset(tlc.Configurable):

    def __init__( self, filepath: str, **kwargs ):
        tlc.Configurable.__init__(**kwargs)
        self._filepath = filepath
        self._sd: Optional[SD] = None

    @property
    def file(self) -> str:
        return self._filepath

    @property
    def sd(self) -> SD:
        if self._sd == None:
            self._sd = SD( self._filepath, SDC.READ )
        return self._sd

    def to_zarr(self,
            store: Union[MutableMapping, str, Path] = None,
            chunk_store: Union[MutableMapping, str, Path] = None,
            mode: str = None,
            synchronizer=None,
            group: str = None,
            encoding: Mapping = None,
            append_dim: Hashable = None,
            region: Mapping[str, slice] = None
        ):
        data_vars: Dict[str,xa.DataArray] = {}
        attrs = {}
        coords = {}
        for (akey, aval) in self._sd.attributes().items():
            if akey.startswith("CoreMetadata"):
                pass
            else:
                attrs[akey] = aval

        for dskey, (dims,shape,index) in self._sd.datasets().items():
            modis_sds: SDS = self._sd.select(dskey)
            sdata = modis_sds.get()
            sattrs = modis_sds.attributes()
            data_vars[dskey] = xa.DataArray( sdata, coords, dims, dskey, sattrs )

        dset = xa.Dataset( data_vars, attrs=attrs )
        dset.to_zarr( store, chunk_store, mode, synchronizer, group, encoding, True, True, append_dim, region )
