import boto3
bucket = "eis-dh-hydro"
key = "projects/eis_freshwater/swang9.OL_1km.2013.1/SURFACEMODEL/LIS_HIST.d01.zarr/.zattrs"
client = boto3.client('s3')
client.delete_object( Bucket=bucket, Key=key )