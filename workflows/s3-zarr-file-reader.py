from intake_xarray.xzarr import ZarrSource
import xarray as xa

urlpath = '/discover/nobackup/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013/SURFACEMODEL/201302/LIS_HIST_201302010000.d01.nc'
xzSource: ZarrSource = ZarrSource( urlpath )
data: xa.Dataset = xzSource.to_dask()

print( f"attrs: {data.attrs}" )

for id, var in data.data_vars.items():
    print( f"   {id}{var.dims}: shape={var.shape}" )


# SurfElev_tavg = data['SurfElev_tavg'][100]

# print( f"  SurfElev_tavg[100].shape = {SurfElev_tavg.shape}" )