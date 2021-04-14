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

    def _open_partition(self, ipart: int) -> xa.Dataset:
        raise NotImplementedError()

    def _get_partition(self, ipart: int ) -> xa.Dataset:
        if ipart not in self._parts:
            self._parts[ipart] = self._open_partition(ipart)
        return self._parts[ipart]

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

    def _translate_file(self, ipart: int, **kwargs ) -> str:
        overwrite = kwargs.get('cache_overwrite', True )
        file_specs = self._file_list[ipart]
        local_file_path =  self.get_downloaded_filepath( file_specs.get("resolved") )
        (base_path, file_ext ) = os.path.splitext( os.path.basename(local_file_path) )
        xds: xa.Dataset = self._open_partition(ipart)
        if file_ext in [ ".nc", ".nc4" ]:
            nc_file_path = local_file_path
            print(f"Opening translated file {nc_file_path}")
        else:
            ncfile_name = base_path + ".nc"
            nc_file_path = os.path.join(self.cache_dir, ncfile_name)
            if overwrite or not os.path.exists(nc_file_path):
                print(f"Creating translated file {nc_file_path}")
                xds.to_netcdf( nc_file_path, "w" )
        file_specs[ 'translated'] = nc_file_path
        self.update_varspecs(ipart, file_specs, xds)
        xds.close()
        return nc_file_path

    def update_varspecs(self, ipart: int, file_specs: Dict[str,str], xds: xa.Dataset ):
        Varspec.addFile(ipart, file_specs['translated'] )
        self._file_list[ipart].update( file_specs )
        for vid, xar in xds.items():
            vspec = self._varspecs.setdefault(vid, Varspec(vid, xar.dims))
            vspec.add_instance(ipart, xar.shape)
        for vid in self._varspecs.keys():
            if vid not in xds.keys():
                self._intermittent_vars.add(vid)

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
        print( f"Completed translate (parallel={parallel}) in {time.time()-t0} sec, result = {dsparts}")
        return dsparts

    def to_dask(self) -> xa.Dataset:
        return self.read()

    def _preprocess_for_export(self, vlist: List[str], ds: xa.Dataset):
        print(f"Preprocessed vars for dataset with attrs {ds.attrs}")
        new_vars = {}
        merge_axis_val = ds.attrs[self.merge_dim]
        self._ds_attr_map[ merge_axis_val ] = ds.attrs
        for vname in vlist:
            xar = ds[vname]
            nvar = xar.expand_dims({self.merge_dim: np.array([merge_axis_val])}, 0)
            print(f" -- {vname}: shape={nvar.shape}, dims={nvar.dims}")
            new_vars[vname] = nvar
        return xa.Dataset(new_vars)

    def test_for_equality(self, attvals: List[Any]):
        if ( len(attvals) != self.nparts ): return False
        if isinstance( attvals[0], np.ndarray ):
            return all( (x == attvals[0]).all() for x in attvals)
        else:
            return all( (x == attvals[0]) for x in attvals)

    def _get_merged_attrs( self ) -> Dict:
        merged_attrs = dict()
        for axval, attrs in self._ds_attr_map.items():
            for k,v in attrs.items():
                att_dict = merged_attrs.setdefault( k, dict() )
                att_dict[ axval ] = v
        for attr_name, att_map in  merged_attrs.items():
            attvals = list(att_map.values())
            if self.test_for_equality( attvals ):
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
        mds = self._merge_variable_lists( mvars, merge_dim, self._preprocess_for_export )
        cds = self._merge_variable_lists( cvars, concat_dim )
        rds = mds if (cds is None) else cds if (mds is  None) else mds.merge( cds )
        return rds

    def _merge_variable_lists(self, mvars: List[str], merge_dim: str, preprocess: Callable = None ):
        mds: Optional[xa.Dataset] = None
        sep_char= '.'
        file_lists: Dict[str,List] = {}
        for var in mvars:
            plist: List[str] = self._varspecs[var].non_empty_files(merge_dim)
            file_lists.setdefault( sep_char.join(plist), []).append(var)
        for (fkey, vlist) in file_lists.items():
            plist = fkey.split(sep_char)
            flist = [ Varspec.file_list[ int(ip)] for ip in plist ]
            print( f"MERGING vars {vlist} using files {flist}")
            mds1 = xa.open_mfdataset( flist, concat_dim=merge_dim, data_vars=vlist, preprocess=partial(preprocess,vlist) )
            mds = mds1 if mds is None else mds.merge(mds1)
        print(f" --> merge_variable_lists with mvars {mvars} and merge_dim={merge_dim}\n ** file_lists = {file_lists}\n ** mds = {mds}")
        return mds

    # def _merge_datasets1(self, dset_paths: List[str], concat_dim: str, **kwargs ) -> xa.Dataset:
    #     concat_vars, merge_vars = dict(), dict()
    #     merge_dim = kwargs.get( 'merge_dim',self.merge_dim )
    #     print( "Merge datasets: ")
    #     coords = {}
    #     for dset_path in dset_paths:
    #         ds: xa.Dataset = xa.open_dataset(dset_path)
    #         coords.update( ds.coords )
    #         merge_axis_val = ds.attrs[self.merge_dim]
    #         self._ds_attr_map[merge_axis_val] = ds.attrs
    #         for vid, xar in ds.items():
    #             try:
    #                 ic = xar.dims.index( concat_dim )
    #                 if xar.shape[ic] > 0:
    #                     concat_vars.setdefault( vid, [] ).append( xar )
    #                     xar.attrs[ merge_dim ] = merge_axis_val
    #             except ValueError:  # concat_dim not in xar.dims:
    #                 xar = xar.expand_dims({self.merge_dim: np.array([merge_axis_val])}, 0)
    #                 merge_vars.setdefault(vid, []).append( xar )
    #     result_vars = {}
    #     print( "Create merged variables")
    #     for vid, cvars in concat_vars.items(): result_vars[vid] = xa.concat( cvars, dim=concat_dim )
    #     for vid, mvars in merge_vars.items():  result_vars[vid] = xa.concat( mvars, dim=merge_dim )
    #     print( "Create merged dataset")
    #     result_dset = xa.Dataset(concat_vars, coords, self._get_merged_attrs() )
    #     return result_dset

    def get_cache_path(self, path: str ) -> str:
        from eis_smce.data.storage.s3 import s3m
        if path.startswith("s3:"):
            (bucket, item) = s3m().parse(path)
            path = f"{self._cache_dir}/{item}"
        return path


    def export( self, path: str, **kwargs ) -> EISZarrSource:
        from eis_smce.data.storage.s3 import s3m
        self.merge_dim = kwargs.get( 'merge_dim', self.merge_dim )
        concat_dim = kwargs.get( 'concat_dim', None )
        group = kwargs.get( 'group', None )
        self._ds_attr_map = collections.OrderedDict()
        location = os.path.dirname(path)

        if kwargs.get('merge', True ):
            try:
                self.translate( **kwargs )
                merged_dataset: xa.Dataset = self._merge_datasets( concat_dim=concat_dim )
                merged_dataset.attrs.update( self._get_merged_attrs() )
                print(f"Exporting to zarr file: {path}")
                local_path = self.get_cache_path(path)
                merged_dataset.to_zarr( local_path, mode="w", group = group )
                s3m().upload_files( local_path, path )
                zsrc = EISZarrSource(path)
            except Exception as err:
                print(f"Merge ERROR: {err}")
                traceback.print_exc()
                print(f"\n\nMerge failed, exporting files individually to {location}")
                zsrc = self._multi_export( location, **kwargs )
        else:
            zsrc = self._multi_export( location, **kwargs )

        return zsrc

    def _multi_export(self, location, **kwargs ) -> EISZarrSource:
        sources = []
        for i in range(self.nparts):
            file_spec = self._file_list[i]
            file_path = file_spec["translated"]
            file_name = os.path.splitext(os.path.basename(file_path))[0]
            source = NetCDFSource(file_path)
            zpath = f"{location}/{file_name}.zarr"
            print(f"Exporting to zarr file: {zpath}")
            source.export(zpath, mode="w")
            zs = EISZarrSource(zpath)
            sources.append(zs)
        return sources[0]                           # TODO: Create zarr group

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