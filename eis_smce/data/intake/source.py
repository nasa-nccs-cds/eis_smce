from intake.source.base import DataSource, Schema
import collections, json, shutil, math
from datetime import datetime
from eis_smce.data.storage.local import SegmentedDatasetManager
from intake.source.utils import reverse_format
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable, MutableMapping, Set
from functools import partial
import dask.delayed, boto3, os, io, traceback
from eis_smce.data.intake.zarr.source import EISZarrSource
import intake, zarr, numpy as np
import dask.bag as db
import time, logging,  xarray as xa
import intake_xarray as ixa   # Need this import to register 'xarray' container.
from eis_smce.data.common.base import eisc, EISConfiguration, EISSingleton as eiss

def dsort( d: Dict ) -> Dict: return { k:d[k] for k in sorted(d.keys()) }
def has_char(string: str, chars: str): return 1 in [c in string for c in chars]
def parse_url( urlpath: str) -> Tuple[str, str]:
    ptoks = urlpath.split(":")[-1].strip("/").split("/")
    return ( ptoks[0], "/".join( ptoks[1:] ) )


class EISDataSource( ):
    logger = EISConfiguration.get_logger()

    def __init__(self, input: str, **kwargs ):
        super(EISDataSource, self).__init__()
        self.urlpath = input
        self.segment_manager = SegmentedDatasetManager()
        self._parts: Dict[int,xa.Dataset] = {}
        self._schema: Schema = None
        self.pspec = None
        self.segment_manager.process_files( self.urlpath, **kwargs )

    def _open_partition(self, ipart: int) -> xa.Dataset:
        raise NotImplementedError()

    def _get_partition(self, ipart: int ) -> xa.Dataset:
        return self._open_partition(ipart)

    def get_file_list( self, vlist: Set[str], ibatch: int ) -> List[str]:
        file_spec_list:  List[Dict[str,str]] = self.segment_manager.get_file_specs( vlist )
        Nf = len( file_spec_list )
        (istart,istop)  = (0, Nf) if (ibatch < 0) else (ibatch, min( ibatch + self.batch_size(), Nf ))
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

    @classmethod
    def preprocess( cls, pspec: Dict, ds: xa.Dataset )-> xa.Dataset:
        logger = EISConfiguration.get_logger()
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

    @classmethod
    def log_dset(cls, label: str, dset: xa.Dataset ):
        ostr = io.StringIO("")
        dset.info(ostr)
        cls.logger.info(f" DATASET: {label}-> {ostr.getvalue()} ")
        cls.logger.info(f"    *** CHUNKS:   ***   ")
        for (vid, v) in dset.variables.items():
            cls.logger.info(f"    --> {vid}: {v.chunks}")

    def read( self, **kwargs ) -> xa.Dataset:
        merge_dim = eisc().get( 'merge_dim' )
        chunks: Dict[str, int] = eisc().get('chunks', {merge_dim: 1})
        var_list: Set[str] = kwargs.get('vlist', None)
        ibatch = kwargs.get( 'ibatch', -1 )
        file_list = self.get_file_list( var_list, ibatch )
        self.pspec = dict( pattern=self.urlpath, merge_dim=merge_dim, files=file_list,
                           dynamic_metadata_ids=self.segment_manager.get_dynamic_attributes(),
                           sname = self.segment_manager.get_segment_name( var_list ), **kwargs )
        t0 = time.time()
        ds0: xa.Dataset = xa.open_mfdataset( file_list, concat_dim=merge_dim, coords="minimal", data_vars=var_list,
                                            preprocess=partial( self.preprocess, self.pspec ), parallel = True, chunks=chunks )
        dset = ds0.chunk( chunks )
        mdim = dset[merge_dim].values
        dset.attrs['_files_'] = file_list
        print( f"Reading merged dataset[{ibatch}] from {len(file_list)} files:" )
        print( f" --> File Range: '{os.path.basename(file_list[0])}' -> '{os.path.basename(file_list[-1])}'" )
        print( f" --> {merge_dim} range: {mdim[0]} -> {mdim[-1]}")
        self.logger.info( f"Completed merge in {time.time()-t0} secs, chunks = {dict(dset.chunks)}" )
        return dset

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
        mds: xa.Dataset = self.to_dask( **kwargs )
#        input_files = mds['_eis_source_path'].values.tolist()
        input_files = mds.attrs['_files_']
        for aId in self.pspec['dynamic_metadata_ids']: mds.attrs.pop( aId, "" )
        zargs = dict( compute=False ) # , consolidated=True )
        if init: zargs['mode'] = 'w'
        else:    zargs['append_dim'] = eisc().get( 'merge_dim' )
        store = self.get_cache_path( path, self.pspec )
        with xa.set_options( display_max_rows=100 ):
            self.logger.info( f" merged_dset -> zarr: {store}\n   -------------------- Merged dataset batch[{ibatch}] -------------------- \n{mds}\n")
        print( f"{'Writing' if init else 'Appending'} {len(input_files)} files from batch[{ibatch}] to zarr file: {store}"   )
        mds.to_zarr( store, **zargs )
        mds.close(); del mds
        self.logger.info( f"Completed batch[{ibatch}] zarr initialization. " )
        return input_files

    def partition_chunk_size(self):
        merge_dim = eisc().get( 'merge_dim' )
        chunks: Dict[str, int] = eisc().get( 'chunks', {merge_dim: 1} )
        chunk_size = chunks.get( merge_dim, 1 )
        return chunk_size

    def partition_list( self, lst: List ) -> int:
        chunk_size = self.partition_chunk_size()
        for i in range(0, len(lst), chunk_size):
            yield lst[i: i + chunk_size]

    def batch_size(self) -> int:
        batch_size_suggestion = eisc().get( 'batch_size', 100 )
        chunk_size = self.partition_chunk_size()
        chunks_per_batch: int = round( batch_size_suggestion/chunk_size )
        return chunks_per_batch * chunk_size

    def export(self, output_url: str, **kwargs):
        try:
            from eis_smce.data.storage.s3 import s3m
            from eis_smce.data.common.cluster import dcm
            path = eiss.item_path( output_url )
            parallel = kwargs.get( 'parallel', False )
            for vlist in self.segment_manager.get_vlists():
                print( f"Processing vlist: {vlist}")
                file_spec_list: List[Dict[str, str]] = self.segment_manager.get_file_specs(vlist)
                nfiles = len( file_spec_list )
                for ib in range( 0, nfiles, self.batch_size() ):
                    t0 = time.time()
                    batch_files = self.create_storage_item( path, ibatch=ib, vlist=vlist, **kwargs )
                    current_batch_size, t1 = len(batch_files), time.time()
                    ispecs = [ dict( file_index=ic, input_path=file_spec_list[ic]['resolved'] ) for ic in range( ib, ib+current_batch_size ) ]
                    cspecs = list( self.partition_list( ispecs ) )
                    if parallel:
                        self.logger.info(f"Exporting parallel batch {ib} with {current_batch_size} files over {len(cspecs)} procs/chunks to: {path}")
                        results = dcm().client.map( partial( EISDataSource._export_partition, path, self.pspec ), cspecs )
                        dcm().client.compute(results, sync=True)
                    else:
                        self.logger.info(f"Exporting batch {ib} with {current_batch_size} files to: {path}")
                        for cspec in cspecs:
                            EISDataSource._export_partition( path, self.pspec, cspec )
                    print( f"Completed processing batch {ib} ({current_batch_size} files) in {(time.time()-t0)/60:.1f} (init: {(t1-t0)/60:.1f}) min.")

        except Exception  as err:
            self.logger.error(f"Exception in export: {err}")
            self.logger.error(traceback.format_exc())

    @classmethod
    def test_for_NaN(cls, dset: xa.Dataset, vname: str, x0: int, y0: int, dx: int, dy: int ):
        test_var: xa.DataArray = dset.data_vars[vname]
        Nan_counts = []
        for iy in range( y0, test_var.shape[1], dy ):
            for ix in range( x0, test_var.shape[2], dx ):
                NaN_ts = np.isnan( test_var[:,iy,ix].squeeze() )
                ncnt = np.count_nonzero( NaN_ts )
                rval = "." if ncnt == test_var.shape[0] else "N"
                Nan_counts.append( "*" if ncnt == 0 else rval )
        cls.logger.info(f'\nNaN count for {vname}{test_var.shape}: {"".join( Nan_counts )}')

    @classmethod
    def _export_partition(cls, output_path:str, pspec: Dict, ispecs: List[Dict], parallel=False ):
        store = EISDataSource.get_cache_path( output_path, pspec )
        file_indices = [ ispec['file_index'] for ispec in ispecs ]
        input_files = [ ispec['input_path'] for ispec in ispecs ]
        merge_dim, t0 = pspec.get( 'merge_dim' ), time.time()
        print( f"Exporting files {file_indices[0]} -> {file_indices[-1]}")
        region = {merge_dim: slice(file_indices[0], file_indices[-1] + 1)}
        cls.logger.info(f'xa.open_mfdataset: file_indices={file_indices}, concat_dim = {merge_dim}, region= {region}')
        cdset = xa.open_mfdataset( input_files, concat_dim=merge_dim, preprocess=partial( EISDataSource.preprocess, pspec ), parallel=parallel )
        cls.log_dset( '_export_partition_parallel', cdset )
        cls.test_for_NaN( cdset, "FloodedArea_tavg", 50, 50, 100, 100 )
        cdset.to_zarr( store, mode='a', region=region )
        print(f" -------------------------- Export[{file_indices[0]} -> {file_indices[-1]}] complete in {(time.time()-t0)/60} min -------------------------- ")

    @classmethod
    def write_catalog( cls, zpath: str, **kwargs ):
        catalog_args = { k: kwargs.pop(k,None) for k in [ 'discription', 'name', 'metadata'] }
        zsrc = EISZarrSource( zpath, **kwargs )
        zsrc.yaml( **catalog_args )

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
        self._schema = None
        self._varspecs = {}
        self._intermittent_vars = set()