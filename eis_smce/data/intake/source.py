from intake.source.base import DataSource, Schema
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import dask.delayed
import xarray as xa
import intake_xarray as ixa   # Need this import to register 'xarray' container.

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
        self._mds = None
        self.nparts = -1

    def _open_file(self, file_specs: Dict[str,str] ) -> xa.Dataset:
        raise NotImplementedError()

    def _get_partition(self, i):
        if i not in self._parts:
            self._parts[i] = self._open_file( self._file_list[i] )
            self._mds = None
        return self._parts[i]

    def read(self) -> xa.Dataset:
        self._load_metadata()
        dsparts = [dask.delayed(self._get_partition)(i) for i in range(self.nparts)]
        self._mds = dask.delayed(self._merge_parts)( dsparts, "number_of_active_fires" )
        return self._mds.compute()

    def _concat_dsets(self, dsets: List[xa.Dataset], concat_dim: str, existing_dim: bool, **kwargs  ) -> xa.Dataset:
        if len(dsets) == 1: return dsets[0]
        if existing_dim:
            filter_op = lambda dims: (dims and (concat_dim in dims))
            cdim = concat_dim
        else:
            filter_op = lambda dims: (dims and (concat_dim not in dims))
            cdim = kwargs.get( 'new_dim', "sample" )
        concat_parts = [ ds.filter_by_attrs(DIMS=filter_op) for ds in dsets ]
        return xa.concat( concat_parts, dim=cdim, data_vars="minimal", combine_attrs="drop_conflicts", compat='override', coords="all" )

    def _merge_parts( self, parts: List[xa.Dataset], concat_dim: str, merge_dim: str = "sample"  ):
        fparts = list( filter( lambda x: (x is not None), parts ) )
        concat_ds = self._concat_dsets( fparts, concat_dim, True )
        merge_ds = self._concat_dsets( fparts, concat_dim, False )
        return xa.merge( [ concat_ds, merge_ds ], combine_attrs= "drop_conflicts" )

    def to_dask(self):
        self._load_metadata()
        if self._mds is None:
            self._mds = dask.delayed(self._merge_parts)( self._parts, "number_of_active_fires" )
        return self._mds

    def _get_schema(self):
        self.urlpath = self._get_cache(self.urlpath)[0]
        if self._schema == None:
            if self.urlpath.startswith( "s3:"):
                from eis_smce.data.storage.s3 import s3m
                self._file_list = s3m().get_file_list( self.urlpath )
            else:
                from eis_smce.data.storage.local import lfm
                self._file_list = lfm().get_file_list( self.urlpath )
            self.nparts = len(self._file_list)
            print( f"Created file list from {self.urlpath} with {self.nparts} parts")
            ds0 =  self._get_partition( 0 )
            metadata = {
                'dims': dict(ds0.dims),
                'data_vars': {k: list(ds0[k].coords) for k in ds0.data_vars.keys()},
                'coords': tuple(ds0.coords.keys()),
            }
            metadata.update( ds0.attrs )
            self._schema = Schema( datashape=None, dtype=None, shape=None, npartitions=self.nparts, extra_metadata=metadata)
        return self._schema


    def close(self):
        """Delete open file from memory"""
        self._file_list = None
        self._parts = {}
        self._ds = None
        self._schema = None
