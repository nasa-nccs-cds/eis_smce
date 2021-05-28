from eis_smce.data.intake.source import EISDataSource
import boto3, math, shutil
import rioxarray as rxr
import rasterio as rio
import xarray as xa
import numpy as np
from pyhdf.SD import SD, SDC, SDS
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import os

NC_CHAR_REPLACEMENTS = { "/": "-" }

def nc_id( sds_id: str ):
    for zc,rc in NC_CHAR_REPLACEMENTS.items():  sds_id = sds_id.replace(zc,rc)
    return sds_id

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

        def _open_partition(self, ipart: int) -> xa.Dataset:
            # Use rasterio/GDAL to read the metadata and pyHDF to read the variable data.
            file_specs = nc_keys( self._file_list[ipart] )
            rfile_path = file_specs.pop("resolved")
            file_path = self.get_downloaded_filepath(rfile_path)
            self.logger.info(f"Reading file {file_path} (remote: {rfile_path}) with specs {file_specs}")
            (base_path, file_ext) = os.path.splitext(os.path.basename(file_path))
            if file_ext in [ '.nc', '.nc4']:
                xds = xa.open_dataset(file_path)
            elif file_ext in [ '.hdf' ]:
                rxr_dsets = rxr.open_rasterio(file_path)
                dsattr = nc_keys( rxr_dsets[0].attrs if isinstance(rxr_dsets, list) else rxr_dsets.attrs )
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
    #                    self.logger.info(f"Creating DataArray {dsid}, DIMS = {attrs['DIMS']}, file = {file_path}")
                        data_vars[nc_vid] = xa.DataArray(data, xcoords, xdims, nc_vid, attrs)
                    except Exception as err:
                        self.logger.error(  f"Error extracting data for sds {dsid}, xdims={xdims}, xcoords={xcoords}, shape={shape}: {err}")
                        self.logger.error(f"sd_dims.items() = {sd_dims.items()}, coords={coords}, dims={dims}")

                xds = xa.Dataset(data_vars, coords, dsattr)
                sd.end()
            else:
                raise Exception( f"Unsupported file extension: {file_ext}")

            xds.attrs['remote_file'] = rfile_path
            xds.attrs['local_file'] = file_path
            xds.attrs.update(file_specs)
            return xds

