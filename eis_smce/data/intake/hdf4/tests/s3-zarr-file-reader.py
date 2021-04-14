from intake_xarray.xzarr import ZarrSource
import xarray as xa

urlpath = 's3://eis-dh-hydro/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013/ROUTING/IS_RST_HYMAP2_router.d01.zarr'
xzSource: ZarrSource = ZarrSource( urlpath )
data: xa.Dataset = xzSource.read()

print( f"attrs: {data.attrs}" )

for id, var in data.data_vars.items():
    print( f"   {id}{var.dims}: shape={var.shape}" )