from eis_smce.data.storage.s3 import s3m

bucketname = 'eis-dh-fire'
pattern = "mod14/raw/MOD14.{id0}.{id1}.061.{id2}.hdf"
glob_pattern = "mod14/raw/MOD14.*.hdf"

files_list = s3m().get_file_list( bucketname, pattern )

for entry in files_list:
    print( entry )

