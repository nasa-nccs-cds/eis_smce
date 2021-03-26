import intake

item_key = 'mod14/raw/MOD14.A2020296.0645.061.2020348134049.hdf'
bucketname = 'eis-dh-fire'

ds = intake.open_hdf4( f"s3://{bucketname}/{item_key}")
print(ds.__class__)