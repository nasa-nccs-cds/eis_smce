from eis_smce.data.intake.source import EISDataSource
import boto3, math, shutil
import rioxarray as rxr
import rasterio as rio
import xarray as xa
import numpy as np
from pyhdf.SD import SD, SDC, SDS
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import os

def nc_id( sds_id: str):
    rv_id = sds_id.replace(":", "_").replace(" ", "_").replace("-", "_")
    return rv_id

def nc_keys( sds_dict: Dict[str,Any] ):
    return { nc_id( did ): v for (did,v) in sds_dict.items() }

class HDF4Source( EISDataSource ):

        name = 'hdf4'

        def __init__(self, data_url: str, xarray_kwargs=None, metadata=None, **kwargs):
            self.urlpath = data_url
            self.xarray_kwargs = xarray_kwargs or {}
            super(HDF4Source, self).__init__(metadata=metadata, **kwargs)

        def _get_data(self, sds: SDS, shape: List[int]) -> np.ndarray:
            ndim = len(shape)
            is_empty = (ndim == 0) or (0 in shape)
            if is_empty:
                return np.empty([0])
            elif ndim == 1:
                return np.array(sds[:]).reshape(shape)
            elif ndim == 2:
                return np.array(sds[:, :]).reshape(shape)
            elif ndim == 3:
                return np.array(sds[:, :, :]).reshape(shape)
            elif ndim == 4:
                return np.array(sds[:, :, :, :]).reshape(shape)
            elif ndim == 5:
                return np.array(sds[:, :, :, :, :]).reshape(shape)

        def _open_file( self, ipart: int  ) -> xa.Dataset:
            from eis_smce.data.storage.s3 import s3m
            # Use rasterio/GDAL to read the metadata and pyHDF to read the variable data.
            file_specs = nc_keys( self._file_list[ipart] )
            print(f"Opening file[{ipart}] from specs: {file_specs}")
            file_path = rfile_path = file_specs.pop("resolved")
            print(f"Resolved: {file_path}")
            if rfile_path.startswith("s3"):
                file_path = s3m().download(rfile_path, self.cache_dir)
                print(f"Reading file {file_path} (downloade3d from {rfile_path})")
            else:
                print(f"Reading file {file_path}")
            rxr_dsets = rxr.open_rasterio(file_path)
            dsattr = nc_keys( rxr_dsets[0].attrs if isinstance(rxr_dsets, list) else rxr_dsets.attrs )
            dsattr.update(file_specs)
            sd: SD = SD(file_path, SDC.READ)
            dsets = sd.datasets().keys()
            dims = {}
            coords = {}
            data_vars = {}
            for dsid in dsets:
                nc_vid = nc_id( dsid )
                sds = sd.select(dsid)
                sd_dims: Dict[str,int] = sds.dimensions()
                nc_dims: Dict[str,int] = nc_keys( sd_dims )
                attrs = nc_keys( sds.attributes() )
                attrs['DIMS'] = list(nc_dims.keys())
                for did, dsize in nc_dims.items():
                    if did in dims:
                        assert dsize == dims[did], f"Dimension size discrepancy for dimension {did}"
                    else:
                        dims[did] = dsize
                    if did not in coords.keys():
                        coords[did] = np.arange(0, dsize)

                xcoords = [coords[did] for did in nc_dims.keys()]
                xdims = nc_dims.keys()
                shape = [dims[did] for did in nc_dims.keys()]
                try:
                    data = self._get_data(sds, shape)
                    print(f"Creating DataArray {dsid}, DIMS = {attrs['DIMS']}, file = {file_path}")
                    data_vars[nc_vid] = xa.DataArray(data, xcoords, xdims, nc_vid, attrs)
                    print(f"  var[{nc_vid}] attrs: {attrs.keys()}")
                except Exception as err:
                    print(
                        f"Error extracting data for sds {dsid}, xdims={xdims}, xcoords={xcoords}, shape={shape}: {err}")
                    print(f"sd_dims.items() = {sd_dims.items()}, coords={coords}, dims={dims}")

            xds = xa.Dataset(data_vars, coords, dsattr)
            xds.attrs['remote_file'] = rfile_path
            xds.attrs['local_file'] = file_path
            print( f"  dset attrs: {xds.attrs.keys()}")
            sd.end()

            return xds




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
