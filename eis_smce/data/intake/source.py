from intake.source.base import DataSource, Schema
import collections, json, shutil
import traitlets.config as tlc, random, string
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable, MutableMapping
from functools import partial
import dask.delayed, boto3, os, traceback
from dask.distributed import Client, LocalCluster
from eis_smce.data.intake.zarr.source import EISZarrSource
import intake, zarr, numpy as np
import dask.bag as db
import time, logging,  xarray as xa
import intake_xarray as ixa   # Need this import to register 'xarray' container.
from eis_smce.data.common.base import eisc

def dsort( d: Dict ) -> Dict: return { k:d[k] for k in sorted(d.keys()) }

class EISDataSource( DataSource ):
    """Common behaviours for plugins in this repo"""
    version = 0.1
    container = 'xarray'
    partition_access = True
    default_merge_dim = "sample"

    def __init__(self, **kwargs ):
        super(EISDataSource, self).__init__( **kwargs )
        self.logger = eisc().logger
        self._file_list: List[ Dict[str,str] ] = None
        self._parts: Dict[int,xa.Dataset] = {}
        self._schema: Schema = None
        self._ds: xa.Dataset = None
        self.nchunks = -1

    def _open_partition(self, ipart: int) -> xa.Dataset:
        raise NotImplementedError()

    def _get_partition(self, ipart: int ) -> xa.Dataset:
        return self._open_partition(ipart)

    def get_file_path(self, ipart: int ) -> str:
        rfile_path = self._file_list[ipart].get("resolved")
        return self.get_downloaded_filepath( rfile_path )

    def get_file_list(self) -> List[str]:
        return [self.get_file_path(ip) for ip in range(self.nchunks)]

    def get_local_file_path(self, data_url: str):
        if data_url.startswith("s3"):
            toks = data_url.split("/")
            file_name = toks[-1]
            data_url = os.path.join( eisc().cache_dir, file_name)
        return data_url

    def get_downloaded_filepath(self, file_path: str ):
        from eis_smce.data.storage.s3 import s3m
        if file_path.startswith("s3"): file_path = s3m().download( file_path, eisc().cache_dir )
        return file_path

    def to_dask( self, **kwargs ) -> xa.Dataset:
        return self.read( **kwargs )

    @staticmethod
    def preprocess( merge_dim: str, dset: xa.Dataset )-> xa.Dataset:
        eisc().logger.info( f"preprocess: merge_dim = {merge_dim}\n    dset dims = {list(dset.dims)}\n    dset items = {list(dset.keys())}\n    dset coords = {list(dset.coords.keys())}\n    dset path = {dset.encoding['source']}" )
        ds: xa.Dataset = dset.assign( eis_source_path = dset.encoding["source"] )
        return ds

    def read( self, **kwargs ) -> xa.Dataset:
        self._load_metadata()
        merge_dim = kwargs.get('merge_dim', self.default_merge_dim)
        file_list = self.get_file_list()
        t0 = time.time()
        self.logger.info( f"Reading merged dataset from {len(file_list)} files, merge_dim = {merge_dim}")
        rv = xa.open_mfdataset( file_list, concat_dim=merge_dim, coords="minimal", data_vars="all", preprocess=partial(self.preprocess,merge_dim), parallel = True )
        self.logger.info( f"Completed merge in {time.time()-t0} secs" )
        return rv

    def test_for_equality(self, attvals: List[Any]):
        if ( len(attvals) != self.nchunks): return False
        if isinstance( attvals[0], np.ndarray ):
            return all( (x == attvals[0]).all() for x in attvals)
        else:
            return all( (x == attvals[0]) for x in attvals)

    @staticmethod
    def get_cache_path( path: str ) -> str:
        from eis_smce.data.storage.s3 import s3m
        if path.startswith("s3:"):
            (bucket, item) = s3m().parse(path)
            path = f"{eisc().cache_dir}/{item}"
        return path

    @staticmethod
    def get_store( path: str, clear: bool = False, **kwargs ):
        from eis_smce.data.storage.s3 import s3m
        use_cache = kwargs.get( 'cache', True )
        store = EISDataSource.get_cache_path(path) if use_cache else s3m().get_store(path,clear)
        return store

    def create_storage_item(self, path: str, **kwargs ) -> xa.Dataset:
        store = self.get_store(path, True)
        mds: xa.Dataset = self.to_dask(**kwargs)
        self.logger.info( f" merged_dset -> zarr: {store}\n   -------------------- Merged dataset: -------------------- \n{mds}\n")
        mds.to_zarr(store, mode="w", compute=False, consolidated=True)
        return mds

    def export(self, path: str, **kwargs ) -> EISZarrSource:
        try:
            from eis_smce.data.storage.s3 import s3m
            from eis_smce.data.common.cluster import dcm
            store = self.get_store(path, True)
            mds: xa.Dataset = self.create_storage_item( store, **kwargs )
            use_cache = kwargs.get( "cache", True )
            chunks_per_part = 10

            client: Client = dcm().client
            compute = (client is None)
            zsources = []
            self.logger.info( f"Exporting paritions to: {path}, compute = {compute}, vars = {list(mds.keys())}" )
            for ic in range(0, self.nchunks, chunks_per_part):
                t0 = time.time()
                zsources.append( EISDataSource._export_partition( store, mds, ic, chunks_per_part, compute=compute, **kwargs ) )
                self.logger.info(f"Completed partition export in {time.time()-t0} sec")

            if not compute:
                zsources = client.compute( zsources, sync=True )
            mds.close()

            if( use_cache and path.startswith("s3:") ):
                self.logger.info(f"Uploading zarr file to: {path}")
                s3m().upload_files( path )

            return EISZarrSource(path)
        except Exception  as err:
            self.logger.error(f"Exception in export: {err}")
            self.logger.error(traceback.format_exc())

    def export_parallel(self, path: str, **kwargs ) -> EISZarrSource:
        try:
            from eis_smce.data.storage.s3 import s3m
            from eis_smce.data.common.cluster import dcm
            mds: xa.Dataset = self.create_storage_item( path, **kwargs )
            input_files = mds['eis_source_path'].values
            mds.close()
            use_cache = kwargs.get( "cache", True )
            client: Client = dcm().client

            zsources = []
            self.logger.info( f"Exporting paritions to: {path}, vars = {list(mds.keys())}" )
            for ic in range(0, self.nchunks):
                zsources.append( dask.delayed( EISDataSource._export_partition_parallel )( input_files[ic], path, ic, **kwargs ) )

            zsc = client.compute( zsources, sync=True )

            if( use_cache and path.startswith("s3:") ):
                self.logger.info(f"Uploading zarr file to: {path}")
                s3m().upload_files( path )

            return EISZarrSource(path)
        except Exception  as err:
            self.logger.error(f"Exception in export: {err}")
            self.logger.error(traceback.format_exc())

    @staticmethod
    def _export_partition(  store: Union[str,MutableMapping], mds: xa.Dataset, chunk_offset: int, nchunks: int, **kwargs ):
        merge_dim = kwargs.get( 'merge_dim', EISDataSource.default_merge_dim )
        region = { merge_dim: slice(chunk_offset, chunk_offset + nchunks) }
        eisc().logger.info( f"Exporting {nchunks} chunks at offset {chunk_offset} to store {store}" )
        dset = mds[region]
        return dset.to_zarr( store, mode='a', region=region )

    @staticmethod
    def _export_partition_parallel(  input_path: str, output_path:str, chunk_index: int,  **kwargs ):
        store = EISDataSource.get_store( output_path, False )
        merge_dim = kwargs.get( 'merge_dim', EISDataSource.default_merge_dim )
        region = { merge_dim: slice(chunk_index, chunk_index + 1) }
        dset = xa.open_dataset( input_path )
        return dset.to_zarr( store, mode='a', region=region )

    def get_zarr_source(self, zpath: str ):
        zsrc = EISZarrSource(zpath)
        zsrc.yaml()

    def print_bucket_contents(self, bucket_prefix: str ):
        s3 = boto3.resource('s3')
        for bucket in s3.buckets.all():
            if bucket.name.startswith( bucket_prefix ):
                self.logger.info(f'** {bucket.name}:')
                for obj in bucket.objects.all():
                    self.logger.info(f'   -> {obj.key}: {obj.__class__}')

    def _collect_parts( self, parts: List[Any]  ) -> dask.bag.Bag:
        fparts = list( filter( lambda x: (x is not None), parts ) )
        if ( len( fparts ) == 1 ): return fparts[0]
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
            self.nchunks = len(self._file_list)
            self.logger.info( f"Created file list from {self.urlpath} with {self.nchunks} parts")
            ds0 =  self._get_partition( 0 )
            metadata = {
                'dims': dict(ds0.dims),
                'data_vars': {k: list(ds0[k].coords) for k in ds0.data_vars.keys()},
                'coords': tuple(ds0.coords.keys()),
            }
            metadata.update( ds0.attrs )
            self._schema = Schema(datashape=None, dtype=None, shape=None, npartitions=self.nchunks, extra_metadata=metadata)
        return self._schema


    def close(self):
        """Delete open file from memory"""
        self._file_list = None
        self._parts = {}
        self._ds = None
        self._schema = None
        self._varspecs = {}
        self._intermittent_vars = set()