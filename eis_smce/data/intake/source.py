from intake.source.base import DataSource, Schema
import collections, json
import traitlets.config as tlc, random, string
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable, MutableMapping
from functools import partial
import dask.delayed, boto3, os, traceback
from intake_xarray.netcdf import NetCDFSource
from eis_smce.data.intake.zarr.source import EISZarrSource
import intake, zarr, numpy as np
import dask.bag as db
import time, xarray as xa
import intake_xarray as ixa   # Need this import to register 'xarray' container.

def dsort( d: Dict ) -> Dict: return { k:d[k] for k in sorted(d.keys()) }

class Varspec:
    file_list: Dict[int,str] = {}

    def __init__( self, vid: str, dims: List[str] ):
        self.dims: List[str] = dims
        self.vid = vid
        self.instances: Dict[ int, List[int] ] = {}

    def non_empty_files(self, dim: str ) -> List[str]:
        di = self.dim_index(dim)
#        return [ self.file_list[ip] for (ip, shape) in self.instances.items() if (shape[di] > 0) ]
        nefiles = []
        for (ip, shape) in self.instances.items():
            if (shape[di] > 0):
                print( f"NON-empty file: var={self.vid}, merge_dim={dim}, shape = {shape}, file={self.file_list[ip]}")
                nefiles.append( str(ip) )
        nefiles.sort()
        return nefiles

    def add_instance(self, ipart: int, shape: List[int] ):
        self.instances[ipart] = shape

    @classmethod
    def addFile( cls, ipart: int, file_path ):
        cls.file_list[ipart] = file_path

    def dim_index( self, dim: str ) -> int:
        try:
           return self.dims.index(dim)
        except ValueError:  # concat_dim not in xar.dims:
            return -1

    @property
    def nfiles(self) -> int:
        return len( self.instances )

class EISDataSource( DataSource ):
    """Common behaviours for plugins in this repo"""
    version = 0.1
    container = 'xarray'
    partition_access = True

    def __init__(self, **kwargs ):
        self._cache_dir = kwargs.pop('cache_dir', os.path.expanduser("~/.eis_smce/cache"))
        super(EISDataSource, self).__init__( **kwargs )
#        dask.config.set(scheduler='threading')
        self._file_list: List[ Dict[str,str] ] = None
        self._parts: Dict[int,xa.Dataset] = {}
        self.merge_dim = "sample"
        self._schema: Schema = None
        self._ds: xa.Dataset = None
        self.nparts = -1

    @property
    def cache_dir(self):
        return self._cache_dir

    def _open_partition(self, ipart: int) -> xa.Dataset:
        raise NotImplementedError()

    def _get_partition(self, ipart: int ) -> xa.Dataset:
        if ipart not in self._parts:
            xds: xa.Dataset = self._open_partition(ipart)
            if self.merge_dim in xds.dims.keys():
                self._parts[ipart] = xds
            else:
                merge_coord_val = xds.attrs.get( self.merge_dim, ipart )
                merge_coord = { self.merge_dim: np.array(merge_coord_val) }
                self._parts[ipart] = xds.expand_dims(merge_coord, 0)
        xds = self._parts[ipart]
        return xds

    def get_file_path(self, ipart: int ) -> str:
        rfile_path = self._file_list[ipart].get("resolved")
        return self.get_downloaded_filepath( rfile_path )

    def get_file_list(self) -> List[str]:
        return [ self.get_file_path(ip) for ip in range(self.nparts) ]

    def get_local_file_path(self, data_url: str):
        if data_url.startswith("s3"):
            toks = data_url.split("/")
            file_name = toks[-1]
            data_url = os.path.join( self.cache_dir, file_name)
        return data_url

    def get_downloaded_filepath(self, file_path: str ):
        from eis_smce.data.storage.s3 import s3m
        if file_path.startswith("s3"): file_path = s3m().download( file_path, self.cache_dir )
        return file_path

    # def translate( self, **kwargs ) -> List[str]:
    #     t0 = time.time()
    #     parallel = kwargs.pop('parallel', False )
    #     self._load_metadata()
    #     print( "Transforming inputs")
    #     if  parallel:
    #         dsparts_delayed = [ dask.delayed(self._translate_file)( i, **kwargs ) for i in range(self.nparts)]
    #         dsparts = dask.compute( *dsparts_delayed )
    #     else:
    #         dsparts = [ self._translate_file( i, **kwargs ) for i in range(self.nparts) ]
    #     print( f"Completed translate (parallel={parallel}) in {time.time()-t0} sec, result = {dsparts}")
    #     return dsparts

    def to_dask(self) -> xa.Dataset:
        return self.read()

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
            path = f"{self._cache_dir}/{item}"
        return path

    def export( self, path: str, **kwargs ) -> EISZarrSource:
        from eis_smce.data.storage.s3 import s3m
        self.merge_dim = kwargs.get( 'merge_dim', self.merge_dim )
        self._load_metadata()
        # concat_dim = kwargs.get( 'concat_dim', None )
        # group = kwargs.get( 'group', None )
        # location = os.path.dirname(path)
        local_path = self.get_cache_path(path)
        mds = xa.open_mfdataset( self.get_file_list(), concat_dim=self.merge_dim, coords= "minimal" )
        print(f" merged_dset[{self.merge_dim}] -> zarr: {local_path}\n   mds = {mds}")
        mds.to_zarr( local_path, compute=False )

        for ip in range( self.nparts ):
            xds: xa.Dataset= self._get_partition( ip )
            region = { self.merge_dim: slice( ip, ip+1 ) }
            print(f" Exporting P{ip}" )
#            print(f" P{ip}: export_to_zarr[{self.merge_dim}]: xds: {xds}")
            xds.to_zarr( local_path, region=region )
            xds.close()

        print(f"Uploading zarr file to: {path}")
        s3m().upload_files( local_path, path )
        zsrc = EISZarrSource(path)
        return zsrc

    def get_zarr_source(self, zpath: str ):
        zsrc = EISZarrSource(zpath)
        zsrc.yaml()

    def print_bucket_contents(self, bucket_prefix: str ):
        s3 = boto3.resource('s3')
        for bucket in s3.buckets.all():
            if bucket.name.startswith( bucket_prefix ):
                print(f'** {bucket.name}:')
                for obj in bucket.objects.all():
                    print(f'   -> {obj.key}: {obj.__class__}')

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
        self._varspecs = {}
        self._intermittent_vars = set()