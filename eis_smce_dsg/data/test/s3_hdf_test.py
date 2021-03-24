import boto3
import xarray as xa
import numpy as np
from pyhdf.SD import SD, SDC, SDS
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import os

def get_data( sds: SDS, shape ):
    ndim = len( shape )
    if ndim == 0: return 0
    elif ndim == 1: return np.array( sds[:] ).reshape( shape )
    elif ndim == 2: return np.array( sds[:,:] ).reshape( shape )
    elif ndim == 3: return np.array( sds[:,:,:] ).reshape( shape )
    elif ndim == 4: return np.array( sds[:,:,:,:] ).reshape( shape )
    elif ndim == 5: return np.array( sds[:,:,:,:,:] ).reshape( shape )

modis_s3_item = 'mod14/raw/MOD14.A2020298.1835.061.2020348153757.hdf'
bucketname = 'eis-dh-fire'
local_cache_dir = "/home/jovyan/cache"
client = boto3.client('s3')

file_name = modis_s3_item.split("/")[-1]
modis_filepath = os.path.join( local_cache_dir, file_name )
if not os.path.exists(modis_filepath): client.download_file( bucketname, modis_s3_item, modis_filepath )
print( f"Reading file {modis_filepath}")

sd = SD( modis_filepath, SDC.READ )
dsets = sd.datasets().keys()
print( f"METADATA keys = {sd.attributes().keys()} ")

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
    print( f" {dsid}: {sd_dims}" )
    for did, dsize in sd_dims.items():
        if did in dims: assert dsize == dims[did], f"Dimension size discrepancy for dimension {dim}"
        else:           dims[ did ] = dsize
        if did not in coords:
            coords[did]= np.arange( 0, dsize )

    xcoords = [ coords[did] for did in sd_dims.keys() ]
    xdims = sd_dims.keys()
    shape = [ dims[did] for did in sd_dims.keys() ]
    data = get_data( sds, shape  )

    xda = xa.DataArray( data, xcoords, xdims, dsid, attrs )
    data_vars[ dsid ] = xda

xds = xa.Dataset( data_vars, coords, dsattr )

print( f"Constructed Dataset")
