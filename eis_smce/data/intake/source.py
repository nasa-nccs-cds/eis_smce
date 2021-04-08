from intake.source.base import DataSource, Schema
import collections, json
import traitlets.config as tlc, random, string
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable, MutableMapping
import dask.delayed, boto3, os, traceback
from intake_xarray.netcdf import NetCDFSource
from intake_xarray.xzarr import ZarrSource
import intake, zarr, numpy as np
import dask.bag as db
import time, xarray as xa
import intake_xarray as ixa   # Need this import to register 'xarray' container.

def dsort( d: Dict ) -> Dict: return { k:d[k] for k in sorted(d.keys()) }

class Varspec:
    file_list: Dict[int,str] = {}

    def __init__( self, dims: List[str] ):
        self.dims: List[str] = dims
        self.instances: Dict[ int, List[int] ] = {}

    def non_empty_files(self, dim: str ):
        di = self.dim_index(dim)
#        return [ self.file_list[ip] for (ip, shape) in self.instances.items() if (shape[di] > 0) ]
        nefiles = []
        for (ip, shape) in self.instances.items():
            if (shape[di] > 0):
                print( f"NON-empty file[{dim}]: shape = {shape} for {self.file_list[ip]}")
                nefiles.append( self.file_list[ip] )
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
        super(EISDataSource, self).__init__( **kwargs )
        dask.config.set(scheduler='threading')
        self._cache_dir = kwargs.get( 'cache_dir', os.path.expanduser( "~/.eis_smce/cache") )
        self._file_list: List[ Dict[str,str] ] = None
        self._parts: Dict[int,xa.Dataset] = {}
        self._schema: Schema = None
        self._ds: xa.Dataset = None
        self.nparts = -1
        self._ds_attr_map = collections.OrderedDict()
        self.merge_dim = "sample"
        self._instance_cache = None
        self._varspecs: Dict[str,Varspec] = {}
        self._intermittent_vars = set()

    @property
    def cache_dir(self):
        return self._cache_dir

    def _open_file(self, ipart: int ) -> xa.Dataset:
        raise NotImplementedError()

    def _get_partition(self, ipart: int ) -> xa.Dataset:
        if ipart not in self._parts:
            self._parts[ipart] = self._open_file( ipart )
        return self._parts[ipart]

    def get_local_file_path(self, data_url: str):
        if data_url.startswith("s3"):
            toks = data_url.split("/")
            file_name = toks[-1]
            data_url = os.path.join( self.cache_dir, file_name)
        return data_url

    def _translate_file(self, ipart: int, **kwargs ) -> str:
        overwrite = kwargs.get('cache_overwrite', False )
        file_specs = self._file_list[ipart]
        local_file_path =  self.get_local_file_path( file_specs.get("resolved") )
        ncfile_name = os.path.splitext( os.path.basename(local_file_path) )[0] + ".nc"
        nc_file_path = os.path.join(self.cache_dir, ncfile_name)
        print(f"Creating translated file {nc_file_path}")
        xds: xa.Dataset = self._open_file(ipart)
        file_path = xds.attrs['local_file']
        xds.attrs['local_file'] = nc_file_path
        if 'sample' not in list(xds.attrs.keys()): xds.attrs['sample'] = ipart
        Varspec.addFile(ipart, nc_file_path)
        for vid, xar in xds.items():
            vspec = self._varspecs.setdefault(vid, Varspec(xar.dims))
            vspec.add_instance(ipart, xar.shape)
        for vid in self._varspecs.keys():
            if vid not in xds.keys():
                self._intermittent_vars.add(vid)
        if overwrite or not os.path.exists(nc_file_path):
            print( f"Translating file {file_path} to {nc_file_path}" )
            xds.to_netcdf( nc_file_path, "w" )
        xds.close()
        self._file_list[ipart]["translated"] = nc_file_path
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
        t0 = time.time()
        parallel = kwargs.get('parallel', False )
        self._load_metadata()
        print( "Transforming inputs")
        if  parallel:
            dsparts_delayed = [ dask.delayed(self._translate_file)( i, **kwargs ) for i in range(self.nparts)]
            dsparts = dask.compute( *dsparts_delayed )
        else:
            dsparts = [ self._translate_file( i, **kwargs ) for i in range(self.nparts) ]
        print( f"Completed translate (parallel={parallel}) in {time.time()-t0} sec")
        return dsparts

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
        merged_attrs = dict()
        for axval, attrs in self._ds_attr_map.items():
            for k,v in attrs.items():
                att_dict = merged_attrs.setdefault( k, dict() )
                att_dict[ axval ] = v
        for attr_name, att_map in  merged_attrs.items():
            attvals = list(att_map.values())
            if (len(attvals) == self.nparts) and all(x == attvals[0] for x in attvals):
                   merged_attrs[ attr_name ] = attvals[0]
            else:  merged_attrs[ attr_name ] = str( dsort(att_map) )
        return merged_attrs

    def _merge_datasets(self, concat_dim: str, **kwargs ) -> xa.Dataset:
        merge_dim = kwargs.get( 'merge_dim',self.merge_dim )
        print( "Merge datasets: ")
        cvars, mvars, cfiles, mfiles = [], [], [], []
        for vid, vspec in self._varspecs.items():
            if vspec.dim_index( concat_dim ) == -1:
                mvars.append( vid )
            else:
                cvars.append( vid )
        mds: Optional[xa.Dataset] = None
        if len( mvars ) > 0:
            files = self._varspecs[mvars[0]].non_empty_files(merge_dim)
            mds = xa.open_mfdataset( files, concat_dim=merge_dim, data_vars=mvars, preprocess=self._preprocess_for_export )
        if len( cvars ) > 0:
            files = self._varspecs[cvars[0]].non_empty_files(merge_dim)
            cds: xa.Dataset = xa.open_mfdataset( files, concat_dim=concat_dim, data_vars=cvars )
            mds = mds.merge( cds ) if (mds is not None) else cds
        return mds

    def _merge_datasets1(self, dset_paths: List[str], concat_dim: str, **kwargs ) -> xa.Dataset:
        concat_vars, merge_vars = dict(), dict()
        merge_dim = kwargs.get( 'merge_dim',self.merge_dim )
        print( "Merge datasets: ")
        coords = {}
        for dset_path in dset_paths:
            ds: xa.Dataset = xa.open_dataset(dset_path)
            coords.update( ds.coords )
            merge_axis_val = ds.attrs[self.merge_dim]
            self._ds_attr_map[merge_axis_val] = ds.attrs
            for vid, xar in ds.items():
                try:
                    ic = xar.dims.index( concat_dim )
                    if xar.shape[ic] > 0:
                        concat_vars.setdefault( vid, [] ).append( xar )
                        xar.attrs[ merge_dim ] = merge_axis_val
                except ValueError:  # concat_dim not in xar.dims:
                    xar = xar.expand_dims({self.merge_dim: np.array([merge_axis_val])}, 0)
                    merge_vars.setdefault(vid, []).append( xar )
        result_vars = {}
        print( "Create merged variables")
        for vid, cvars in concat_vars.items(): result_vars[vid] = xa.concat( cvars, dim=concat_dim )
        for vid, mvars in merge_vars.items():  result_vars[vid] = xa.concat( mvars, dim=merge_dim )
        print( "Create merged dataset")
        result_dset = xa.Dataset(concat_vars, coords, self._get_merged_attrs() )
        return result_dset

    def export( self, path: str, **kwargs ) -> List[ZarrSource]:
        from eis_smce.data.intake.catalog import CatalogManager, cm
        self.merge_dim = kwargs.get( 'merge_dim', self.merge_dim )
        concat_dim = kwargs.get( 'concat_dim', None )
        self._ds_attr_map = collections.OrderedDict()
        location = os.path.dirname(path)

        if kwargs.get('merge', True ):
            try:
                self.translate( **kwargs )
                merged_dataset: xa.Dataset = self._merge_datasets( concat_dim=concat_dim )
                merged_dataset.attrs.update( self._get_merged_attrs() )
                merged_dataset.to_zarr( path, mode="w" )
                print(f"Exporting to zarr file: {path}")
                zsrc = [ ZarrSource(path) ]
            except Exception as err:
                print(f"Merge ERROR: {err}")
                traceback.print_exc()
                print(f"\n\nMerge failed, exporting files individually to {location}")
                zsrc = self._multi_export( location )
        else:
            zsrc = self._multi_export( location )

        if kwargs.get( 'update_cat', True ):
            for zs in zsrc: cm().addEntry(zs)
        return zsrc

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
        self._varspecs = {}
        self._intermittent_vars = set()