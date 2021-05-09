from intake.source.base import DataSource, Schema
import collections, json, shutil, math
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler
from eis_smce.data.common.cluster import dcm, cim
from datetime import datetime
from eis_smce.data.storage.local import SegmentedDatasetManager
from intake.source.utils import reverse_format
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable, MutableMapping, Set
from functools import partial
import dask.delayed, boto3, os, traceback
from dask.distributed import Client, LocalCluster
from eis_smce.data.intake.zarr.source import EISZarrSource
import intake, zarr, numpy as np
import dask.bag as db
import time, logging,  xarray as xa
import intake_xarray as ixa   # Need this import to register 'xarray' container.
from eis_smce.data.common.base import eisc, EISConfiguration, EISSingleton as eiss

def dsort( d: Dict ) -> Dict: return { k:d[k] for k in sorted(d.keys()) }
def has_char(string: str, chars: str): return 1 in [c in string for c in chars]
def eis_item_path( path: str) -> str: return path.split(":")[-1].replace("//", "/").replace("//", "/")
def parse_url( urlpath: str) -> Tuple[str, str]:
    ptoks = urlpath.split(":")[-1].strip("/").split("/")
    return ( ptoks[0], "/".join( ptoks[1:] ) )

class EISDataSource( ):
    logger = EISConfiguration.get_logger()

    def __init__(self, input: str ):
        super(EISDataSource, self).__init__()
        self.urlpath = input
        self.segment_manager = SegmentedDatasetManager()
        self._parts: Dict[int,xa.Dataset] = {}
        self._schema: Schema = None
        self._ds: xa.Dataset = None
        self.pspec = None
        self.batch_size = eisc().get( 'batch_size', 1000 )
        self.chunk_size = self.segment_manager.process_files( self.urlpath )

    def _open_partition(self, ipart: int) -> xa.Dataset:
        raise NotImplementedError()

    def _get_partition(self, ipart: int ) -> xa.Dataset:
        return self._open_partition(ipart)

    def get_file_list( self, vlist: Set[str], ibatch: int ) -> List[str]:
        file_spec_list:  List[Dict[str,str]] = self.segment_manager.get_file_specs( vlist )
        Nf = len( file_spec_list )
        (istart,istop)  = (0, Nf) if (ibatch < 0) else (ibatch, min( ibatch + self.batch_size, Nf ))
        return [ file_spec_list[ip].get("resolved") for ip in range(istart,istop) ]

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
        dynamic_metadata = dict( _eis_source_path = source_file_path )
        for aId in pspec['dynamic_metadata_ids']:
            att_val = ds.attrs.pop(aId,None)
            if att_val is not None:
                dynamic_metadata[f"_{aId}"] = att_val
        ds = ds.assign( dynamic_metadata )
        new_vlist = list( pspec['vlist'] ) + list( dynamic_metadata.keys() )
        if merge_dim not in list( ds.coords.keys() ):
            ds = ds.drop_vars( set( ds.data_vars.keys() ).difference( new_vlist ) )
            filepath_pattern = eis_item_path(pattern)
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
            rds = ds.expand_dims( dim={ merge_dim: merge_coord }, axis=0 )
        else:
            vlist = {}
            for vid in new_vlist:
                xv = ds[vid]
                if merge_dim not in list( xv.coords.keys() ):
                    xv = xv.expand_dims(dim=merge_dim, axis=0)
                vlist[vid] = xv
            rds = xa.Dataset( vlist, ds.coords, ds.attrs )
        return rds

    def read( self, **kwargs ) -> xa.Dataset:
        merge_dim = eisc().get( 'merge_dim' )
        var_list: Set[str] = kwargs.get('vlist', None)
        ibatch = kwargs.get( 'ibatch', -1 )
        file_list = self.get_file_list( var_list, ibatch )
        self.pspec = dict( pattern=self.urlpath, merge_dim=merge_dim, chunk_size=self.chunk_size, files=file_list,
                           dynamic_metadata_ids=self.segment_manager.get_dynamic_attributes(),
                           nchunks = self.segment_manager.get_segment_size( var_list ),
                           sname = self.segment_manager.get_segment_name( var_list ), **kwargs )
        t0 = time.time()
        rv: xa.Dataset = xa.open_mfdataset( file_list, concat_dim=merge_dim, coords="minimal", data_vars=var_list,
                                            preprocess=partial( self.preprocess, self.pspec ), parallel = True )
        mdim = rv[merge_dim].values
        print( f"Reading merged dataset[{ibatch}] from {len(file_list)} files:" )
        print( f" --> File Range: '{os.path.basename(file_list[0])}' -> '{os.path.basename(file_list[-1])}'" )
        print( f" --> {merge_dim} range: {mdim[0]} -> {mdim[-1]}")
        self.logger.info( f"Completed merge in {time.time()-t0} secs" )
        return rv

    @staticmethod
    def get_cache_path( path: str, pspec: Dict ) -> str:
        if path.startswith("s3:"):
            (bucket, item) = parse_url(path)
            path = f"{eisc().cache_dir}/{item}"
        return path + pspec['sname'] + ".zarr"

    # @staticmethod
    # def get_store( path: str, clear: bool = False, **kwargs ):
    #     from eis_smce.data.storage.s3 import s3m
    #     use_cache = kwargs.get( 'cache', True )
    #     store = EISDataSource.get_cache_path(path) if use_cache else s3m().get_store(path,clear)
    #     return store

    def create_storage_item(self, path: str, **kwargs ) -> List[str]:
        ibatch =  kwargs.get( 'ibatch', 0 )
        init = ( ibatch == 0 )
#        store = self.get_store( path, True )
        mds: xa.Dataset = self.to_dask( **kwargs )
        input_files = mds['_eis_source_path'].values.tolist()
        for aId in self.pspec['dynamic_metadata_ids']: mds.attrs.pop( aId, "" )
        zargs = dict( compute=False, consolidated=True )
        if init: zargs['mode'] = 'w'
        else:    zargs['append_dim'] = eisc().get( 'merge_dim' )
        store = self.get_cache_path( path, self.pspec )
        with xa.set_options( display_max_rows=100 ):
            self.logger.info( f" merged_dset -> zarr: {store}\n   -------------------- Merged dataset batch[{ibatch}] -------------------- \n{mds}\n")
        print( f"{'Writing' if init else 'Appending'} batch[{ibatch}] to zarr file: {store}"   )
        mds.to_zarr( store, **zargs )
        mds.close(); del mds
        return input_files

    def export_parallel(self, path: str, **kwargs ):
        try:
            from eis_smce.data.storage.s3 import s3m
            from eis_smce.data.common.cluster import dcm
            for vlist in self.segment_manager.get_vlists():
                print( f"Processing vlist: {vlist}")
                file_spec_list: List[Dict[str, str]] = self.segment_manager.get_file_specs(vlist)
                ib = 0
                while True:
                    t0 = time.time()
                    input_files = self.create_storage_item( path, ibatch=ib, vlist=vlist, **kwargs )
                    nfiles, t1 = len(input_files), time.time()
                    self.logger.info( f"Exporting batch {ib} with {nfiles} files to: {path}" )
                    ispecs = [ dict( chunk_index=ic, input_path=file_spec_list[ic]['resolved'] ) for ic in range( ib, ib+nfiles*self.chunk_size, self.chunk_size ) ]
                    with ResourceProfiler(20) as rprof, CacheProfiler() as cprof:
                        results = dcm().client.map(partial(EISDataSource._export_partition_parallel, path, self.pspec), ispecs)
                        dcm().client.compute( results, sync=True )
                        for rp in rprof.results: self.logger.info( f"RP: {rp}" )
                        for cp in cprof.results: self.logger.info( f"CP: {cp}" )
                    print( f"Completed processing batch {ib} ({nfiles}/{self.pspec['nchunks']} files) in {(time.time()-t0)/60:.1f} (init: {(t1-t0)/60:.1f}) min.")
                    ib = ib + self.batch_size*self.chunk_size
                    if ib >= self.pspec['nchunks']: break

        except Exception  as err:
            self.logger.error(f"Exception in export: {err}")
            self.logger.error(traceback.format_exc())

    @classmethod
    def _export_partition_parallel( cls, output_path:str, pspec: Dict, ispec: Dict ):
        store = EISDataSource.get_cache_path( output_path, pspec )
        chunk_index = ispec['chunk_index']
        merge_dim = pspec.get( 'merge_dim' )
        ds0 = xa.open_dataset( ispec['input_path'] )
        region = { merge_dim: slice( chunk_index, chunk_index + pspec['chunk_size'] ) }
        dset = EISDataSource.preprocess( pspec, ds0 )
        mval = dset[merge_dim].values[0]
        fname = os.path.basename( dset['_eis_source_path'].values[0] )
        cls.logger.info(f'**Export: region: {region}, {merge_dim} = {mval}, file = {fname}' )
        dset.to_zarr( store, mode='a', region=region )
        ds0.close(); del ds0; dset.close(); del dset

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

    def equal_attr(self, v0, v1 ) -> bool:
        if v1 is None: return False
        if type(v0) is np.ndarray:    return np.array_equal( v0, v1 )
        if type(v0) is xa.DataArray:  return v0.equals( v1 )
        return v0 == v1

    def close(self):
        """Delete open file from memory"""
        self._parts = {}
        self._ds = None
        self._schema = None
        self._varspecs = {}
        self._intermittent_vars = set()