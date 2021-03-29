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
        self.nparts = -1

    def _open_file(self, file_specs: Dict[str,str] ) -> xa.Dataset:
        raise NotImplementedError()

    def _open_partition(self, i):
        if i not in self._parts:
            self._parts[i] = self._open_file( self._file_list[i] )
        return self._parts[i]

    def _get_partition(self, i):
        part = self._open_partition( i )
        self._ds = self._merge_parts( [self._ds, part] )
        return part

    def read(self) -> xa.Dataset:
        self._load_metadata()
        dsparts = [dask.delayed(self._open_partition)(i) for i in range(self.nparts)]
        self._ds = dask.delayed(self._merge_parts)(dsparts)
        return self._ds.compute()

    def _merge_parts( self, parts: List[xa.Dataset] ):
        fparts = list( filter( lambda x: (x is not None), parts ) )
        print( f"Merging {len(parts)} partitions ({len(fparts)} non-null)")
        if len( fparts ) == 1: return fparts[0]
        return xa.concat( fparts, dim="number_of_active_fires", data_vars = "minimal", combine_attrs= "drop_conflicts" )

    def to_dask(self):
        self._get_schema()
        return self._ds

    def _filter_by_coord( self, ds: xa.Dataset, dim: str ):
        data_vars = []
        coords = {}
        attrs = dict()

        return xa.Dataset(data_vars=None, coords=None, attrs=None)

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
