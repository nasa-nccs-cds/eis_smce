from intake_xarray.xzarr import ZarrSource

urlpath = 's3://eis-dh-fire/mod14/raw/MOD14.A2020303.0515.061.2020349105630.zarr'

xzSource: ZarrSource = ZarrSource( urlpath )

print( xzSource.describe() )