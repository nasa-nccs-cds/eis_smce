import os, xarray as xr
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from intake_xarray.xzarr import ZarrSource

data_batch_url: str = "s3://eis-dh-fire/mod14/raw/MOD14.A2020303.{tod}.061.{sample}.hdf"
# export_path = "s3://eis-dh-fire/mod14/raw/MOD14.A2020303.061.zarr"
export_path = "/home/jovyan/cache/mod14/raw/MOD14.A2020303.061.zarr"

if __name__ == '__main__':
    h4s: HDF4Source = HDF4Source( data_batch_url  )             # Creates source encapsulating all matched files in data_url
    h4s.export( export_path, concat_dim )                                   # Reads all partitions into lazy chunked dask array.

