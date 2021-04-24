from intake.source.base import DataSource, Schema
import collections, json, shutil
import traitlets.config as tlc, random, string
from datetime import datetime
from intake.source.utils import reverse_format
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable, MutableMapping
from functools import partial
import dask.delayed, boto3, os, traceback
from dask.distributed import Client, LocalCluster
from eis_smce.data.intake.zarr.source import EISZarrSource
import intake, zarr, numpy as np
import dask.bag as db
import time, logging,  xarray as xa
import intake_xarray as ixa   # Need this import to register 'xarray' container.
from eis_smce.data.common.base import eisc, EISSingleton as eiss
def dsort( d: Dict ) -> Dict: return { k:d[k] for k in sorted(d.keys()) }
def has_char(string: str, chars: str): return 1 in [c in string for c in chars]

class EISDataSource( DataSource ):
    """Common behaviours for plugins in this repo"""
    version = 0.1
    container = 'xarray'
    partition_access = True
    default_merge_dim = "time"

    def __init__(self, **kwargs ):
        super(EISDataSource, self).__init__( **kwargs )
        self.logger = eisc().logger
        self._file_list: List[ Dict[str,str] ] = None
        self._parts: Dict[int,xa.Dataset] = {}
        self._schema: Schema = None
        self._ds: xa.Dataset = None
        self.nchunks = -1
        self.pspec = None
        self.dynamic_metadata_ids = []

    def _open_partition(self, ipart: int) -> xa.Dataset:
        raise NotImplementedError()

    def _get_partition(self, ipart: int ) -> xa.Dataset:
        return self._open_partition(ipart)

    def get_file_path(self, ipart: int ) -> str:
        rfile_path = self._file_list[ipart].get("resolved")
        return self.get_downloaded_filepath( rfile_path )

    def get_file_list(self) -> List[str]:
        return [self.get_file_path(ip) for ip in range(self.nchunks)]

    def get_file_index_map(self) -> Dict[str, int ]:
        return { self.get_file_path(ip): ip for ip in range(self.nchunks) }

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
    def preprocess( pspec: Dict, ds: xa.Dataset )-> xa.Dataset:
        merge_dim = pspec['merge_dim']
        pattern = pspec['pattern']
        time_format = pspec.get( 'time_format', None )
        source_file_path = ds.encoding["source"]
        dynamic_metadata = dict( eis_source_path = source_file_path )
        for aId in pspec['dynamic_metadata_ids']:
            att_val = str( ds.attrs.pop(aId,"") )
            while aId in ds.keys(): aId = f"{aId}_"
            dynamic_metadata[aId] = att_val
        ds = ds.assign( dynamic_metadata )
        if merge_dim not in list( ds.coords.keys() ):
            filepath_pattern = eiss.item_path(pattern)
            is_glob = has_char(filepath_pattern, "*?[")
            (file_path, file_pattern) = ( os.path.basename(source_file_path), os.path.basename(filepath_pattern)) if is_glob else (source_file_path, filepath_pattern)
            metadata = reverse_format( file_pattern, file_path )
            if merge_dim in metadata.keys():
                merge_coord_val = metadata[ merge_dim ]
                try:
                    if time_format is not None:
                        merge_coord_val = datetime.strptime( merge_coord_val, time_format )
                    merge_coord = np.array([merge_coord_val], dtype='datetime64')
                except ValueError:
                    merge_coord = np.array( [merge_coord_val] )
            else:
                merge_coord = np.array(  pspec['files'].index( source_file_path ) )
            return ds.expand_dims( dim={ merge_dim: merge_coord }, axis=0 )
        else:
            vlist = {}
            for vid, xv in ds.items():
                if merge_dim not in list( xv.coords.keys() ):
                    xv = xv.expand_dims(dim=merge_dim, axis=0)
                vlist[vid] = xv

            return xa.Dataset( vlist, ds.coords, ds.attrs )

    def read( self, **kwargs ) -> xa.Dataset:
        self._load_metadata()
        merge_dim = kwargs.get( 'merge_dim', self.default_merge_dim )
        file_list = self.get_file_list()
        t0 = time.time()
        self.logger.info( f"Reading merged dataset from {len(file_list)} files, merge_dim = {merge_dim}")
        self.pspec = dict( files=file_list, pattern=self.urlpath, merge_dim=merge_dim, dynamic_metadata_ids = self.dynamic_metadata_ids, **kwargs )
        rv: xa.Dataset = xa.open_mfdataset( file_list, concat_dim=merge_dim, coords="minimal", data_vars="all", preprocess=partial( self.preprocess, self.pspec ), parallel = True )
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

    # def export(self, path: str, **kwargs ) -> EISZarrSource:
    #     try:
    #         from eis_smce.data.storage.s3 import s3m
    #         from eis_smce.data.common.cluster import dcm
    #         store = self.get_store(path, True)
    #         mds: xa.Dataset = self.create_storage_item( store, **kwargs )
    #         use_cache = kwargs.get( "cache", True )
    #         chunks_per_part = 10
    #
    #         client: Client = dcm().client
    #         compute = (client is None)
    #         zsources = []
    #         self.logger.info( f"Exporting paritions to: {path}, compute = {compute}, vars = {list(mds.keys())}" )
    #         for ic in range(0, self.nchunks, chunks_per_part):
    #             t0 = time.time()
    #             zsources.append( EISDataSource._export_partition( store, mds, ic, chunks_per_part, compute=compute, **kwargs ) )
    #             self.logger.info(f"Completed partition export in {time.time()-t0} sec")
    #
    #         if not compute:
    #             zsources = client.compute( zsources, sync=True )
    #         mds.close()
    #
    #         if( use_cache and path.startswith("s3:") ):
    #             self.logger.info(f"Uploading zarr file to: {path}")
    #             s3m().upload_files( path )
    #
    #         return EISZarrSource(path)
    #     except Exception  as err:
    #         self.logger.error(f"Exception in export: {err}")
    #         self.logger.error(traceback.format_exc())

    def export_parallel(self, path: str, **kwargs ):
        try:
            from eis_smce.data.storage.s3 import s3m
            from eis_smce.data.common.cluster import dcm
            mds: xa.Dataset = self.create_storage_item( path, **kwargs )
            input_files = mds['eis_source_path'].values
            mds.close()
            client: Client = dcm().client

            tasks = []
            self.logger.info( f"Exporting paritions to: {path}, vars = {list(mds.keys())}" )
            for ic in range(0, self.nchunks):
                tasks.append( dask.delayed( EISDataSource._export_partition_parallel )( input_files[ic], path, ic, self.pspec ) )
            client.compute( tasks, sync=True )

        except Exception  as err:
            self.logger.error(f"Exception in export: {err}")
            self.logger.error(traceback.format_exc())

    # @staticmethod
    # def _export_partition(  store: Union[str,MutableMapping], mds: xa.Dataset, chunk_offset: int, nchunks: int, **kwargs ):
    #     merge_dim = kwargs.get( 'merge_dim', EISDataSource.default_merge_dim )
    #     region = { merge_dim: slice(chunk_offset, chunk_offset + nchunks) }
    #     eisc().logger.info( f"Exporting {nchunks} chunks at offset {chunk_offset} to store {store}" )
    #     dset = mds[region]
    #     return dset.to_zarr( store, mode='a', region=region )

    @staticmethod
    def _export_partition_parallel(  input_path: str, output_path:str, chunk_index: int,  pspec: Dict ):
        logger = logging.getLogger('eis_smce.intake')
        t0 = time.time()
        store = EISDataSource.get_cache_path( output_path )
        merge_dim = pspec.get( 'merge_dim', EISDataSource.default_merge_dim )
        region = { merge_dim: slice(chunk_index, chunk_index + 1) }
        dset = EISDataSource.preprocess( pspec, xa.open_dataset( input_path ) )
        dset.to_zarr( store, mode='a', region=region )
        dset.close()
        logger.info( f"Finished generating zarr chunk in {time.time()-t0} secs: {output_path}")

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
            dsmeta = {}
            ds0 =  self._get_partition( 0 )
            ds1 =  self._get_partition( -1 )
            for k,v in ds0.attrs.items():
                v1 = ds1.attrs.get( k, None )
                if (v1 is None) or ( v1 != ds0.attrs[k] ):  self.dynamic_metadata_ids.append( k )
                else:                                       dsmeta[k] = v
            metadata = {
                'dims': dict(ds0.dims),
                'data_vars': {k: list(ds0[k].coords) for k in ds0.data_vars.keys()},
                'coords': tuple(ds0.coords.keys()),
            }
            metadata.update( dsmeta )
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