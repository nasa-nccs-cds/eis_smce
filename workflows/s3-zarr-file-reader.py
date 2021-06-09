from intake_xarray.xzarr import ZarrSource
from data import s3m
import xarray as xa

bucket = "eis-dh-hydro"
s3_prefix = f"projects/eis_freshwater/swang9.OL_1km.2013.1"
urlpath=f"s3://{bucket}/{s3_prefix}/SURFACEMODEL/LIS_HIST.d01.zarr"

print( f"Reading zarr File: {urlpath} ")
xzSource: ZarrSource = ZarrSource( urlpath )
data: xa.Dataset = xzSource.to_dask()

print( f"attrs: {data.attrs}" )
for id, var in data.data_vars.items():
    print( f"   {id}{var.dims}: shape={var.shape}" )

GPP_tavg: xa.DataArray = data['GPP_tavg'][10]
print( f"  GPP_tavg range = {GPP_tavg.min().values} {GPP_tavg.max().values} ")

print( f"Deleting: {urlpath}")
s3m().delete( urlpath )

