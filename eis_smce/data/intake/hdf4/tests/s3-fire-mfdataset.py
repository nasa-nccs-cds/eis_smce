import os, xarray as xr
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from intake_xarray.xzarr import ZarrSource

data_batch_url: str = "s3://eis-dh-fire/mod14/raw/MOD14.A2020303.{tod}.061.{sample}.hdf"

h4s: HDF4Source = HDF4Source( data_batch_url  )             # Creates source encapsulating all matched files in data_url
ds0: xr.Dataset = h4s.read()                                # Reads all partitions into lazy chunked dask array.

print( ds0 )