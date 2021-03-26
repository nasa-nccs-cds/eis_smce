from intake.source.utils import reverse_format
from intake_xarray.base import DataSourceMixin
import boto3
import xarray as xa
import numpy as np
from pyhdf.SD import SD, SDC, SDS
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import os

class HDF4Source( DataSourceMixin ):

    name = 'hdf4'

    def __init__(self, data_url: str, xarray_kwargs=None, metadata=None, cache_dir=None, **kwargs):
        self.cache_dir = cache_dir or os.path.expanduser("~/.eis_smce/cache")
        self.xarray_kwargs = xarray_kwargs or {}
        self._ds = None
        if data_url.startswith("s3"):   self.download_from_s3( data_url )
        else:                           self.urlpath = data_url
        super(HDF4Source, self).__init__(metadata=metadata, **kwargs)

    def _get_data( self, sds: SDS, shape: List[int] ) -> np.ndarray:
        ndim = len(shape)
        if ndim == 0:       return np.array( 0 )
        elif ndim == 1:     return np.array( sds[:] ).reshape(shape)
        elif ndim == 2:     return np.array( sds[:,:] ) .reshape(shape)
        elif ndim == 3:     return np.array( sds[:,:,:] ) .reshape(shape)
        elif ndim == 4:     return np.array( sds[:,:,:,:] ).reshape(shape)
        elif ndim == 5:     return np.array( sds[:,:,:,:,:] ).reshape(shape)

    def download_from_s3( self, data_url: str, refresh = False ):
        os.makedirs( self.cache_dir, exist_ok=True )
        toks = data_url.split("/")
        file_name = toks[-1]
        self.urlpath = os.path.join( self.cache_dir, file_name )
        if refresh or not os.path.exists(self.urlpath):
            ibuket = 2 if len(toks[1]) == 0 else 1
            bucketname = toks[ibuket]
            s3_item = '/'.join( toks[ibuket + 1:] )
            client = boto3.client('s3')
            client.download_file( bucketname, s3_item, self.urlpath )

    def _open_file(self) -> xa.Dataset:
        print(f"Reading file {self.urlpath}")
        sd = SD(self.urlpath, SDC.READ)
        dsets = sd.datasets().keys()
        dsattr = {}
        for aid, aval in sd.attributes().items():
            dsattr[aid] = aval

        dims = {}
        coords = {}
        data_vars = {}
        for dsid in dsets:
            sds = sd.select(dsid)
            sd_dims = sds.dimensions()
            attrs = sds.attributes()
            print(f" {dsid}: {sd_dims}")
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
                data_vars[dsid] = xa.DataArray(data, xcoords, xdims, dsid, attrs)
            except Exception as err:
                print( f"Error extracting data for sds {dsid}, xdims={xdims}, xcoords={xcoords}, shape={shape}: {err}")
                print(f"sd_dims.items() = {sd_dims.items()}, coords={coords}, dims={dims}")

        xds = xa.Dataset( data_vars, coords, dsattr )
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
