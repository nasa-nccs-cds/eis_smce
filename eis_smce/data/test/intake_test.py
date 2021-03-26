import intake
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from intake_xarray.xzarr import ZarrSource

item_key = 'mod14/raw/MOD14.A2020296.0645.061.2020348134049'    # empty
#  item_key = 'mod14/raw/MOD14.A2020298.1835.061.2020348153757'
bucketname = 'eis-dh-fire'

ds: HDF4Source = intake.open_hdf4( f"s3://{bucketname}/{item_key}.hdf" )
print( ds.yaml() )

zarr_export_path = f"s3://{bucketname}/{item_key}.zarr"
print( f"Exporting to {zarr_export_path}")
eds: ZarrSource = ds.export( zarr_export_path )
print( eds.yaml() )