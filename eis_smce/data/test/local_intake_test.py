import intake
from eis_smce.data.intake.hdf4.drivers import HDF4Source

test_path = "/Users/tpmaxwel/Dropbox/HDF_file/MOD05_L2.A2021080.0000.061.NRT"

ds: HDF4Source = intake.open_hdf4( f"{test_path}.hdf" )
result_source = ds.export( f"{test_path}.zarr" )

print( result_source.__class__ )
print( result_source.yaml() )