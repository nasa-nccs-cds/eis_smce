from intake_xarray.xzarr import ZarrSource
import xarray as xa

bucket = "eis-dh-hydro"
s3_prefix = f"projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013"
urlpath=f"s3://{bucket}/{s3_prefix}/SURFACEMODEL/LIS_HIST.d01.zarr"

xzSource: ZarrSource = ZarrSource( urlpath )
data: xa.Dataset = xzSource.to_dask()

print( f"attrs: {data.attrs}" )

for id, var in data.data_vars.items():
    print( f"   {id}{var.dims}: shape={var.shape}" )


# SurfElev_tavg = data['SurfElev_tavg'][100]

# print( f"  SurfElev_tavg[100].shape = {SurfElev_tavg.shape}" )