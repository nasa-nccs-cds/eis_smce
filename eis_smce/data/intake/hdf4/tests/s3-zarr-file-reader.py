from intake_xarray.xzarr import ZarrSource
import xarray as xa

urlpath = 's3://eis-dh-fire/mod14/raw/MOD14.A2020303.0515.061.2020349105630.zarr'

xzSource: ZarrSource = ZarrSource( urlpath )

data: xa.Dataset = xzSource.read()

print( f"attrs: {data.attrs}" )

for id, var in data.data_vars.items():
    print( f"   {id}{var.dims}: shape={var.shape}" )