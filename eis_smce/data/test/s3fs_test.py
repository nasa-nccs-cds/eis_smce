import s3fs

bucketname = 'eis-dh-fire'
s3f: s3fs.S3FileSystem  = s3fs.S3FileSystem( anon=True )

print( s3f.ls( f"/{bucketname}" ) )

#
#     store = s3fs.S3Map( root=f"{bucketname}/{s3path}/{modis_filename}_test1", s3=s3f, check=False, create=True )