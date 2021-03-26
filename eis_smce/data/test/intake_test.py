import intake
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from intake_xarray.xzarr import ZarrSource

item_key = 'mod14/raw/MOD14.A2020296.0645.061.2020348134049'
bucketname = 'eis-dh-fire'

ds: HDF4Source = intake.open_hdf4( f"s3://{bucketname}/{item_key}.hdf")
print( ds.yaml() )

eds: ZarrSource = ds.export( f"s3://{bucketname}/{item_key}.zarr" )
print( eds.yaml() )