from eis_smce.data.intake.source import EISDataSource, EISDataFileSource
import boto3, math, shutil
import rioxarray as rxr
import rasterio as rio
import xarray as xa
import numpy as np
from pyhdf.SD import SD, SDC, SDS
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import os

class HDF4Source( EISDataSource ):

    name = 'hdf4'

    def __init__(self, data_url: str, xarray_kwargs=None, metadata=None, cache_dir=None, **kwargs):
        self.cache_dir = cache_dir or os.path.expanduser("~/.eis_smce/cache")
        self.urlpath = data_url
        self.xarray_kwargs = xarray_kwargs or {}
        self._ds = None
        super(HDF4Source, self).__init__(metadata=metadata, **kwargs)

    def _get_data( self, sds: SDS, shape: List[int] ) -> np.ndarray:
        ndim, is_empty = len(shape), ( math.prod( shape ) == 0 )
        if is_empty or (ndim == 0): return np.empty( [0] )
        elif ndim == 1:     return np.array( sds[:] ).reshape(shape)
        elif ndim == 2:     return np.array( sds[:,:] ) .reshape(shape)
        elif ndim == 3:     return np.array( sds[:,:,:] ) .reshape(shape)
        elif ndim == 4:     return np.array( sds[:,:,:,:] ).reshape(shape)
        elif ndim == 5:     return np.array( sds[:,:,:,:,:] ).reshape(shape)

    def download_from_s3( self, data_url: str, refresh = True ):
        os.makedirs( self.cache_dir, exist_ok=True )
        toks = data_url.split("/")
        file_name = toks[-1]
        file_path = os.path.join( self.cache_dir, file_name )
        if refresh or not os.path.exists( file_path ):
            ibuket = 2 if len(toks[1]) == 0 else 1
            bucketname = toks[ibuket]
            s3_item = '/'.join( toks[ibuket + 1:] )
            client = boto3.client('s3')
            client.download_file( bucketname, s3_item, file_path )
        return file_path

    def _open_file( self, file_specs: Dict[str,str] ) -> xa.Dataset:
        # Use rasterio/GDAL to read the metadata and pyHDF to read the variable data.
        file_path = rfile_path = file_specs.pop("resolved")
        if rfile_path.startswith("s3"):
               file_path = self.download_from_s3( rfile_path )
               print(f"Reading file {file_path} (downloade3d from {rfile_path})")
        else:  print( f"Reading file {file_path}" )
        rxr_dset: xa.Dataset = rxr.open_rasterio( file_path )
        dsattr = rxr_dset.attrs
        sd: SD = SD( file_path, SDC.READ )
        dsets = sd.datasets().keys()
        dims = {}
        coords = {}
        data_vars = {}
        for dsid in dsets:
            sds = sd.select(dsid)
            sd_dims = sds.dimensions()
            attrs = sds.attributes().copy()
            attrs.update( file_specs )
            attrs['DIMS'] = list( sd_dims.keys() )
            for did, dsize in sd_dims.items():
                if did in dims:   assert dsize == dims[did], f"Dimension size discrepancy for dimension {did}"
                else:             dims[did] = dsize
                if did not in coords.keys():
                    coords[did] = np.arange(0, dsize)

            xcoords = [coords[did] for did in sd_dims.keys()]
            xdims = sd_dims.keys()
            shape = [dims[did] for did in sd_dims.keys()]
            try:
                data = self._get_data(sds, shape)
                print(f"Creating DataArray {dsid}, DIMS = {attrs['DIMS']}, file = {file_path}")
                data_vars[dsid] = xa.DataArray(data, xcoords, xdims, dsid, attrs)
            except Exception as err:
                print( f"Error extracting data for sds {dsid}, xdims={xdims}, xcoords={xcoords}, shape={shape}: {err}")
                print(f"sd_dims.items() = {sd_dims.items()}, coords={coords}, dims={dims}")

        xds = xa.Dataset( data_vars, coords, dsattr )
        xds.attrs[ 'local_file' ] = file_path
        xds.attrs[ 'remote_file'] = rfile_path
        sd.end()
        rxr_dset.close()
        return xds


    def _open_dataset(self):

        # if "*" in url or isinstance(url, list):
        #     _open_dataset = xr.open_mfdataset
        #     if self.pattern:
        #         kwargs.update(preprocess=self._add_path_to_ds)
        #     if self.combine is not None:
        #         if 'combine' in kwargs:
        #             raise Exception("Setting 'combine' argument twice  in the catalog is invalid")
        #         kwargs.update(combine=self.combine)
        #     if self.concat_dim is not None:
        #         if 'concat_dim' in kwargs:
        #             raise Exception("Setting 'concat_dim' argument twice  in the catalog is invalid")
        #         kwargs.update(concat_dim=self.concat_dim)
        # else:

        self._ds = self._open_file()




    # def _add_path_to_ds(self, ds):
    #     """Adding path info to a coord for a particular file
    #     """
    #     var = next(var for var in ds)
    #     new_coords = reverse_format(self.pattern, ds[var].encoding['source'])
    #     return ds.assign_coords(**new_coords)

    # def clear_path( self, path: str ):
    #     import s3fs
    #     if path.startswith("s3:"):
    #         s3f: s3fs.S3FileSystem = s3fs.S3FileSystem(anon=True)
    #         s3f_path = path.split(':')[-1].strip("/")
    #         if s3f.exists(s3f_path):
    #             print( f"Clearing existing s3 item: {s3f_path}")
    #             s3f.delete(s3f_path, recursive=True)
    #         else:
    #             print(f"S3 path clear for writing: {s3f_path}")
    #     elif os.path.exists(path):
    #         print(f"Clearing existing file: {path}")
    #         if os.path.isfile(path):    os.remove(path)
    #         else:                       shutil.rmtree(path)
    #
    # def file_exists( self, path: str ):
    #     import s3fs
    #     if path.startswith("s3:"):
    #         s3f: s3fs.S3FileSystem = s3fs.S3FileSystem(anon=True)
    #         return s3f.exists(path)
    #     else: return os.path.exists(path)

class HDF4FileSource( EISDataFileSource ):

    name = 'hdf4'

    def __init__(self, data_url: str, xarray_kwargs=None, metadata=None, cache_dir=None, **kwargs):
        self.cache_dir = cache_dir or os.path.expanduser("~/.eis_smce/cache")
        self.urlpath = data_url
        self.xarray_kwargs = xarray_kwargs or {}
        super(HDF4FileSource, self).__init__(metadata=metadata, **kwargs)

    def _get_data( self, sds: SDS, shape: List[int] ) -> np.ndarray:
        ndim, is_empty = len(shape), ( math.prod( shape ) == 0 )
        if is_empty or (ndim == 0): return np.empty( [0] )
        elif ndim == 1:     return np.array( sds[:] ).reshape(shape)
        elif ndim == 2:     return np.array( sds[:,:] ) .reshape(shape)
        elif ndim == 3:     return np.array( sds[:,:,:] ) .reshape(shape)
        elif ndim == 4:     return np.array( sds[:,:,:,:] ).reshape(shape)
        elif ndim == 5:     return np.array( sds[:,:,:,:,:] ).reshape(shape)

    def download_from_s3( self, data_url: str, refresh = True ):
        os.makedirs( self.cache_dir, exist_ok=True )
        toks = data_url.split("/")
        file_name = toks[-1]
        file_path = os.path.join( self.cache_dir, file_name )
        if refresh or not os.path.exists( file_path ):
            ibuket = 2 if len(toks[1]) == 0 else 1
            bucketname = toks[ibuket]
            s3_item = '/'.join( toks[ibuket + 1:] )
            client = boto3.client('s3')
            client.download_file( bucketname, s3_item, file_path )
        return file_path

    def _open_file( self, file_specs: Dict[str,str] ) -> xa.Dataset:
        # Use rasterio/GDAL to read the metadata and pyHDF to read the variable data.
        print( f"Opening file from specs: {file_specs}")
        file_path = rfile_path = file_specs.pop("resolved")
        print( f"Resolved: {file_path}")
        if rfile_path.startswith("s3"):
               file_path = self.download_from_s3( rfile_path )
               print(f"Reading file {file_path} (downloade3d from {rfile_path})")
        else:  print( f"Reading file {file_path}" )
        rxr_arrays = rxr.open_rasterio( file_path )
        dsattr = rxr_arrays[0].attrs
        print( "rxr arrays:" )
        for rxr_array in rxr_arrays:
            print( f" --> {rxr_array.__class__}: attrs = {rxr_array.attrs.keys()}" )
        sd: SD = SD( file_path, SDC.READ )
        dsets = sd.datasets().keys()
        dims = {}
        coords = {}
        data_vars = {}
        for dsid in dsets:
            sds = sd.select(dsid)
            sd_dims = sds.dimensions()
            attrs = sds.attributes().copy()
            attrs.update( file_specs )
            attrs['DIMS'] = list( sd_dims.keys() )
            for did, dsize in sd_dims.items():
                if did in dims:   assert dsize == dims[did], f"Dimension size discrepancy for dimension {did}"
                else:             dims[did] = dsize
                if did not in coords.keys():
                    coords[did] = np.arange(0, dsize)

            xcoords = [coords[did] for did in sd_dims.keys()]
            xdims = sd_dims.keys()
            shape = [dims[did] for did in sd_dims.keys()]
            try:
                data = self._get_data(sds, shape)
                print(f"Creating DataArray {dsid}, DIMS = {attrs['DIMS']}, file = {file_path}")
                data_vars[dsid] = xa.DataArray(data, xcoords, xdims, dsid, attrs)
            except Exception as err:
                print( f"Error extracting data for sds {dsid}, xdims={xdims}, xcoords={xcoords}, shape={shape}: {err}")
                print(f"sd_dims.items() = {sd_dims.items()}, coords={coords}, dims={dims}")

        xds = xa.Dataset( data_vars, coords, dsattr )
        xds.attrs[ 'local_file' ] = file_path
        xds.attrs[ 'remote_file'] = rfile_path
        sd.end()
        return xds


    #
    #     # if "*" in url or isinstance(url, list):
    #     #     _open_dataset = xr.open_mfdataset
    #     #     if self.pattern:
    #     #         kwargs.update(preprocess=self._add_path_to_ds)
    #     #     if self.combine is not None:
    #     #         if 'combine' in kwargs:
    #     #             raise Exception("Setting 'combine' argument twice  in the catalog is invalid")
    #     #         kwargs.update(combine=self.combine)
    #     #     if self.concat_dim is not None:
    #     #         if 'concat_dim' in kwargs:
    #     #             raise Exception("Setting 'concat_dim' argument twice  in the catalog is invalid")
    #     #         kwargs.update(concat_dim=self.concat_dim)
    #     # else:
    #
    #     self._ds = self._open_file()




    # def _add_path_to_ds(self, ds):
    #     """Adding path info to a coord for a particular file
    #     """
    #     var = next(var for var in ds)
    #     new_coords = reverse_format(self.pattern, ds[var].encoding['source'])
    #     return ds.assign_coords(**new_coords)

    # def clear_path( self, path: str ):
    #     import s3fs
    #     if path.startswith("s3:"):
    #         s3f: s3fs.S3FileSystem = s3fs.S3FileSystem(anon=True)
    #         s3f_path = path.split(':')[-1].strip("/")
    #         if s3f.exists(s3f_path):
    #             print( f"Clearing existing s3 item: {s3f_path}")
    #             s3f.delete(s3f_path, recursive=True)
    #         else:
    #             print(f"S3 path clear for writing: {s3f_path}")
    #     elif os.path.exists(path):
    #         print(f"Clearing existing file: {path}")
    #         if os.path.isfile(path):    os.remove(path)
    #         else:                       shutil.rmtree(path)
    #
    # def file_exists( self, path: str ):
    #     import s3fs
    #     if path.startswith("s3:"):
    #         s3f: s3fs.S3FileSystem = s3fs.S3FileSystem(anon=True)
    #         return s3f.exists(path)
    #     else: return os.path.exists(path)
