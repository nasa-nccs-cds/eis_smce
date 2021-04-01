import s3fs

bucketname = 'eis-dh-fire'
fs = s3fs.S3FileSystem(anon=True)

print( fs.ls( bucketname ) )