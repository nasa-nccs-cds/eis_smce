from intake.source.base import DataSource, Schema
import collections, json
import traitlets.config as tlc, random, string
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable, MutableMapping
import dask.delayed, boto3, os, traceback
from intake_xarray.netcdf import NetCDFSource
from intake_xarray.xzarr import ZarrSource
import intake, zarr, numpy as np
import dask.bag as db
import pandas as pd
import xarray as xa
import intake_xarray as ixa   # Need this import to register 'xarray' container.

def dsort( d: Dict ): return { (k, d[k]) for k in sorted(d.keys()) }

class EISDataSource( DataSource ):   # , tlc.Configurable
    """Common behaviours for plugins in this repo"""
    version = 0.1
    container = 'xarray'
    partition_access = True
#    _cache_dir = tlc.Unicode( os.path.expanduser( "~/.eis_smce/cache") ).tag(config=True)

    def __init__(self, **kwargs ):
        super(EISDataSource, self).__init__( **kwargs )
        self._cache_dir = kwargs.get( 'cache_dir', os.path.expanduser( "~/.eis_smce/cache") )
        self._file_list: List[ Dict[str,str] ] = None
        self._parts: Dict[int,xa.Dataset] = {}
        self._schema: Schema = None
        self._ds: xa.Dataset = None
        self.nparts = -1
        self._ds_attr_map = collections.OrderedDict()
        self.merge_dim = "sample"
        self._instance_cache = None

    @property
    def cache_dir(self):
        if self._instance_cache is None:
            cid = ''.join( random.choices( string.ascii_uppercase + string.digits, k=8 ) )
            self._instance_cache = os.path.join( self._cache_dir, cid )
            os.makedirs( self._instance_cache )
        return self._instance_cache

    def _open_file(self, ipart: int ) -> xa.Dataset:
        raise NotImplementedError()

    def _get_partition(self, ipart: int ) -> xa.Dataset:
        if ipart not in self._parts:
            self._parts[ipart] = self._open_file( ipart )
        return self._parts[ipart]

    def _translate_file(self, ipart: int, **kwargs ) -> str:
        overwrite = kwargs.get('cache_overwrite', True )
        xds: xa.Dataset = self._open_file( ipart )
        file_path = xds.attrs['local_file']
        ncfile_name = os.path.splitext( os.path.basename(file_path) )[0] + ".nc"
        nc_file_path =  os.path.join( self.cache_dir, ncfile_name )
        if overwrite or not os.path.exists(nc_file_path):
            xds.attrs['local_file'] = nc_file_path
            xds.attrs['sample'] = ipart
            print( f"Translating file {file_path} to {nc_file_path}" )
            xds.to_netcdf( nc_file_path, "w" )
        if kwargs.get('cache_cleanup', False ): os.remove( file_path )
        self._file_list[ipart]["translated"] = nc_file_path
        xds.close()
        return nc_file_path

    def read( self ) -> xa.Dataset:
        if self._ds is None:
            self._load_metadata()
            if self.nparts == 1:
                self._ds = self._get_partition(0)
            else:
                dsparts: List[str] = [ self._translate_file(i) for i in range(self.nparts) ]
                print( f"Opening mfdataset from parts: {dsparts}")
                self._ds = self._merge_files( dsparts )
                print(f"Opened merged dataset")
        return self._ds

    def translate( self, **kwargs ) -> List[str]:
        self._load_metadata()
        dsparts: List[str] = [self._translate_file( i, **kwargs ) for i in range(self.nparts)]
        return dsparts

    def read_delay( self, merge_axis = None ) -> xa.Dataset:
        if self._ds is None:
            self._load_metadata()
            if self.nparts == 1:
                self._ds = self._get_partition(0)
            else:
                dsparts: List[str] = [ dask.delayed(self._translate_file)(i) for i in range(self.nparts) ]
                self._ds = dask.delayed( self._merge_files )( dsparts )
        return self._ds

    def to_dask(self) -> xa.Dataset:
        return self.read()

    def _preprocess_for_export(self, ds: xa.Dataset):
        new_vars = {}
        merge_axis_val = ds.attrs[self.merge_dim]
        self._ds_attr_map[ merge_axis_val ] = ds.attrs
        for name, xar in ds.items():
            new_vars[name] = xar.expand_dims({self.merge_dim: np.array([merge_axis_val])}, 0)
        return xa.Dataset(new_vars)

    def _get_merged_attrs( self ) -> Dict:
        merged_attrs = {}
        for axval, attrs in self._ds_attr_map.items():
            for k,v in attrs.items():
                att_dict = merged_attrs.setdefault( k, {} )
                att_dict[ axval ] = v
        for attr_name, att_map in  merged_attrs.items():
            attvals = list(att_map.values())
            if (len(attvals) == self.nparts) and all(x == attvals[0] for x in attvals):
                   merged_attrs[ attr_name ] = attvals[0]
            else:  merged_attrs[ attr_name ] = json.dumps( dsort(att_map) )
        return merged_attrs

    def export( self, path: str, **kwargs ) -> List[ZarrSource]:
        merge = kwargs.get('merge', True )
        self.merge_dim = kwargs.get( 'merge_dim', 'sample' )
        self._ds_attr_map = collections.OrderedDict()
        location = os.path.dirname(path)
        if merge:
            try:
                inputs = self.translate( **kwargs )
                merged_dataset: xa.Dataset = xa.open_mfdataset( inputs, concat_dim=self.merge_dim, preprocess=self._preprocess_for_export, parallel=kwargs.get('parallel',True) )
                merged_dataset.attrs.update( self._get_merged_attrs() )
                merged_dataset.to_zarr( path, mode="w" )
                print(f"Exporting to zarr file: {path}")
                return [ ZarrSource(path) ]
            except Exception as err:
                print(f"Merge ERROR: {err}")
                traceback.print_exc()
                print(f"\n\nMerge failed, exporting files individually to {location}")
                return self._multi_export( location )
        else:
            return self._multi_export( location )

    def export1( self, path: str, **kwargs ) -> List[ZarrSource]:
        try:
            inputs = self.translate()
            source = NetCDFSource( inputs )
            print(f"Exporting to zarr file: {path}")
            source.export( path, mode="w" )
            print( f"Merged dataset = {source._ds}")
            print( f"Exported merged dataset to {path}, specs = {source.yaml()}")
            return [ ZarrSource(path) ]
        except Exception as err:
            print(f"Merge ERROR: {err}")
            traceback.print_exc()
            location = os.path.dirname(path)
            print(f"\n\nMerge failed, exporting files individually to {location}")
            return self._multi_export( location )

    def _multi_export(self, location ):
        sources = []
        for i in range(self.nparts):
            file_spec = self._file_list[i]
            file_path = file_spec["translated"]
            file_name = os.path.splitext(os.path.basename(file_path))[0]
            source = NetCDFSource(file_path)
            zpath = f"{location}/{file_name}.zarr"
            print(f"Exporting to zarr file: {zpath}")
            source.export(zpath, mode="w")
            sources.append(ZarrSource(zpath))
        return sources

    def get_zarr_source(self, zpath: str ):
        zsrc = ZarrSource(zpath)
        zsrc.yaml()

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

    def _merge_files( self, files: List  ):
        try:
            return dask.delayed( xa.open_mfdataset )( files )
        except Exception as err:
            print(f" These files cannot be merged due to error: {err}")
            return None

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