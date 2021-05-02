from eis_smce.data.storage.s3 import s3m

bucketname = 'eis-dh-fire'
pattern = "mod14/raw/MOD14.{sample}.hdf"
data_url = f"s3://{bucketname}/{pattern}"

files_list = s3m().get_file_lists(data_url, )

for entry in files_list:
    print( entry )

