from intake.source.base import DataSource, Schema
from pathlib import Path
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable, MutableMapping
import dask.delayed, boto3, os
import dask.bag as db
import xarray as xa
from intake.source.zarr import ZarrArraySource
import intake_xarray as ixa   # Need this import to register 'xarray' container.

class EISDataSource(DataSource):
    """Common behaviours for plugins in this repo"""
    version = 0.1
    container = 'xarray'
    partition_access = True

    def __init__(self, **kwargs ):
        super(EISDataSource, self).__init__( **kwargs )
        self._file_list: List[ Dict[str,str] ] = None
        self._parts: Dict[int,xa.Dataset] = {}
        self._schema: Schema = None
        self._ds: xa.Dataset = None
        self.nparts = -1

    def _open_file(self, file_specs: Dict[str,str] ) -> xa.Dataset:
        raise NotImplementedError()

    def _get_partition(self, i):
        if i not in self._parts:
            self._parts[i] = self._open_file( self._file_list[i] )
        return self._parts[i]

    def _export_partition(self, i, **kwargs) -> str:
        overwrite = kwargs.pop( 'overwrite', True )
        group: str = kwargs.pop( 'group', None )
        location: str = kwargs.pop('location', None)
        wmode = "w" if overwrite else "w-"
        if i not in self._parts:
            self._parts[i] = self._open_file( self._file_list[i] )
        remote_input_file: str = self._parts[i].attrs['remote_file']
        if location is None:    store = os.path.splitext(remote_input_file)[0] + ".zarr"
        else:                   store = f"{location}/{os.path.splitext( os.path.basename(remote_input_file) )[0]}.zarr"
        self._parts[i].to_zarr( store, mode=wmode, group=group )
        return store

    def read( self, merge_axis = None ) -> xa.Dataset:
        self._load_metadata()
        dsparts = [dask.delayed(self._get_partition)(i) for i in range(self.nparts)]
        if merge_axis is None:
            mds = dask.delayed(self._collect_parts)( dsparts )
            return mds.compute()
        else:
            mds = dask.delayed(self._merge_parts)( dsparts, merge_axis )
            return mds.compute()

    def export( self, **kwargs ):
        self._load_metadata()
        dsparts = [dask.delayed(self._export_partition)(i,**kwargs) for i in range(self.nparts)]
        stores = dask.delayed(self._collect_parts)( dsparts )
        return stores.compute()

    def print_bucket_contents(self, bucket_prefix: str ):
        s3 = boto3.resource('s3')
        for bucket in s3.buckets.all():
            if bucket.name.startswith( bucket_prefix ):
                print(f'** {bucket.name}:')
                for obj in bucket.objects.all():
                    print(f'   -> {obj.key}: {obj.__class__}')

    def _concat_dsets(self, dsets: List[xa.Dataset], concat_dim: str, existing_dim: bool, **kwargs  ) -> xa.Dataset:
        if len(dsets) == 1: return dsets[0]
        if existing_dim:
            filter_op = lambda dims: (dims and (concat_dim in dims))
            cdim = concat_dim
        else:
            filter_op = lambda dims: (dims and (concat_dim not in dims))
            cdim = kwargs.get( 'new_dim', "sample" )
        concat_parts = [ ds.filter_by_attrs(DIMS=filter_op) for ds in dsets ]
        return xa.concat( concat_parts, dim=cdim, combine_attrs="drop_conflicts", coords="all" )

    def _merge_parts( self, parts: List[xa.Dataset], concat_dim: str, merge_dim: str = "sample"  ):
        fparts = list( filter( lambda x: (x is not None), parts ) )
        concat_ds = self._concat_dsets( fparts, concat_dim, True )
        merge_ds = self._concat_dsets( fparts, concat_dim, False, new_dim=merge_dim )
        return xa.merge( [ concat_ds, merge_ds ], combine_attrs= "drop_conflicts" )

    def _collect_parts( self, parts: List[Any]  ) -> dask.bag.Bag:
        fparts = list( filter( lambda x: (x is not None), parts ) )
        return db.from_sequence( fparts )

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
