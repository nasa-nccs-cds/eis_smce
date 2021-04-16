from intake_xarray.xzarr import ZarrSource
import xarray as xa, numpy as np

bucket = "eis-dh-hydro"
s3_prefix = f"projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013"
urlpath=f"s3://{bucket}/{s3_prefix}/SURFACEMODEL/LIS_HIST.d01.zarr"

xzSource: ZarrSource = ZarrSource( urlpath )
data: xa.Dataset = xzSource.to_dask()

print( f"attrs: {data.attrs}" )

for id, var in data.data_vars.items():
    print( f"   {id}{var.dims}: shape={var.shape}" )


GPP_tavg: np.ndarray = data['GPP_tavg'][10].values
# GPPs = GPP_tavg[50:100:10,50:100:10].values

print( f"  GPP_tavg range = {GPP_tavg.min()} {GPP_tavg.max()} ")

#print( f"  GPP_tavg[100].shape = {GPP_tavg.shape}" )
#print( f"  GPP_tavg sample = {GPP_tavg[50:100:10,50:100:10].values.tolist()}" )