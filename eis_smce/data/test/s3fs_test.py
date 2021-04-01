import s3fs

bucketname = 'demo-bucket-pic'
# bucketname = 'eis-dh-fire'
fs = s3fs.S3FileSystem(anon=True)

print( fs.ls("/") )