from intake.source.base import DataSource, Schema
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import dask.delayed
import xarray as xa

class EISDataSource(DataSource):
    """Common behaviours for plugins in this repo"""
    version = 0.1
    container = 'xarray'
    partition_access = True

    def __init__(self, **kwargs ):
        super(EISDataSource, self).__init__( **kwargs )
        self._file_list: List[ Dict[str,str] ] = None
        self._parts = {}
        self._schema: Schema = None
        self._ds = None
        self.nparts = -1

    def _open_file(self, file_specs: Dict[str,str] ) -> xa.Dataset:
        raise NotImplementedError()

    def _get_partition(self, i):
        return self._load_part( i )

    def _load_part(self, i):
        if i not in self._parts:
            self._parts[i] = self._open_file( self._file_list[i] )
        return self._parts[i]

    def read(self) -> xa.Dataset:
        return self._ds.compute()

    def _merge_parts(self, parts: List[xa.Dataset] ):
        return xa.concat( parts, dim="samples" )

    def _get_schema(self):
        """Make schema object, which embeds xarray object and some details"""
#        from .xarray_container import serialize_zarr_ds

        self.urlpath = self._get_cache(self.urlpath)[0]
        if self._file_list == None:
            if self.urlpath.startswith( "s3:"):
                from eis_smce.data.storage.s3 import s3m
                self._file_list = s3m().get_file_list( self.urlpath )
            else:
                from eis_smce.data.storage.local import lfm
                self._file_list = lfm().get_file_list( self.urlpath )
            self.nparts = len(self._file_list)
            ds0 =  self.read_partition( 0 )
            metadata = {
                'dims': dict(ds0.dims),
                'data_vars': {k: list(ds0[k].coords) for k in ds0.data_vars.keys()},
                'coords': tuple(ds0.coords.keys()),
            }
            metadata.update( ds0.attrs )
            self._schema = Schema(
                datashape=None,
                dtype=None,
                shape=None,
                npartitions=self.nparts,
                extra_metadata=metadata)
            dsparts = [dask.delayed(self._load_part)(i) for i in range(self.nparts)]
            self._ds = dask.delayed( self._merge_parts )( dsparts )
        return self._schema


    def close(self):
        """Delete open file from memory"""
        self._file_list = None
        self._parts = {}
        self._ds = None
        self._schema = None
