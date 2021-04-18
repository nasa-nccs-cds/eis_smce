from intake.source.base import DataSource, Schema
import collections, json, shutil
import traitlets.config as tlc, random, string
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable, MutableMapping
from functools import partial
import dask.delayed, boto3, os, traceback
from intake_xarray.netcdf import NetCDFSource
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

    def __init__(self, **kwargs ):
        super(EISDataSource, self).__init__( **kwargs )
        self.logger = eisc().logger
        self._file_list: List[ Dict[str,str] ] = None
        self._parts: Dict[int,xa.Dataset] = {}
        self.merge_dim = "sample"
        self._schema: Schema = None
        self._ds: xa.Dataset = None
        self.nparts = -1

    def _open_partition(self, ipart: int) -> xa.Dataset:
        raise NotImplementedError()

    def _get_partition(self, ipart: int ) -> xa.Dataset:
        return self._open_partition(ipart)

    def get_file_path(self, ipart: int ) -> str:
        rfile_path = self._file_list[ipart].get("resolved")
        return self.get_downloaded_filepath( rfile_path )

    def get_file_list(self) -> List[str]:
        return [ self.get_file_path(ip) for ip in range(self.nparts) ]

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

    def read( self, **kwargs ) -> xa.Dataset:
        self._load_metadata()
        self.merge_dim = kwargs.get('merge_dim', self.merge_dim)
        file_list = self.get_file_list()
        parallel = kwargs.get( 'parallel_merge', True )
        t0 = time.time()
        self.logger.info( f"Reading merged dataset from {len(file_list)} files, merge_dim = {self.merge_dim}, parallel = {parallel}" )
        rv = xa.open_mfdataset( file_list, concat_dim=self.merge_dim, coords="minimal", data_vars="all", parallel=parallel )
        self.logger.info( f"Completed merge in {time.time()-t0} secs" )
        return rv

    def test_for_equality(self, attvals: List[Any]):
        if ( len(attvals) != self.nparts ): return False
        if isinstance( attvals[0], np.ndarray ):
            return all( (x == attvals[0]).all() for x in attvals)
        else:
            return all( (x == attvals[0]) for x in attvals)

    def get_cache_path(self, path: str ) -> str:
        from eis_smce.data.storage.s3 import s3m
        if path.startswith("s3:"):
            (bucket, item) = s3m().parse(path)
            path = f"{eisc().cache_dir}/{item}"
        return path

    def export(self, path: str, **kwargs ) -> EISZarrSource:
        try:
            from eis_smce.data.storage.s3 import s3m
            use_cache = kwargs.get('cache', False)
            store = self.get_cache_path(path) if use_cache else s3m().get_store(path)
            mds: xa.Dataset = self.to_dask( **kwargs )
            self.logger.info(f" merged_dset[{self.merge_dim}] -> zarr: {store}\n   -------------------- Merged dataset: -------------------- \n{mds}\n")
            mds.to_zarr( store, mode="w", compute=False, consolidated=True )
            dask.config.set(scheduler='threading')

            self.logger.info( f"Exporting paritions to: {path}" )
            for ip in range(0,self.nparts):
                t0 = time.time()
                self.logger.info( f"Exporting partition {ip}")
                self._export_partition( store, mds, self.merge_dim, ip )
                self.logger.info(f"Completed partition export in {time.time()-t0} sec")

            mds.close()

            if( use_cache and path.startswith("s3:") ):
                self.logger.info(f"Uploading zarr file to: {path}")
                s3m().upload_files( store, path )

            return EISZarrSource(path)
        except Exception  as err:
            self.logger.error(f"Exception in export: {err}")
            self.logger.error(traceback.format_exc())

    def _export_partitions( self, store: str, dset: xa.Dataset, merge_dim: str, ipart0: int, nparts: int ):
        self.logger.info(f"Exporting {nparts} partitions at p0={ipart0}")
        t0 = time.time()
        for ip in range(ipart0, ipart0 + nparts):
            self._export_partition( store, dset, merge_dim, ip, False )
        dt = time.time() - t0
        self.logger.info(f"Completed Export in {dt} sec ( {dt/nparts} per partition )")

    @staticmethod
    def _export_partition(  store: str, dset: xa.Dataset, merge_dim: str, ipart: int, compute=True ):
        region = { merge_dim: slice(ipart, ipart + 1) }
        dset[region].to_zarr(store, mode='a', region=region, compute= compute )

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
            self.nparts = len(self._file_list)
            self.logger.info( f"Created file list from {self.urlpath} with {self.nparts} parts")
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
        self._varspecs = {}
        self._intermittent_vars = set()