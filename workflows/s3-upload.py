from data import s3m

local_path = "/discover/nobackup/tpmaxwel/cache/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013/ROUTING/LIS_HIST.d01.zarr"
s3_path = "s3://eis-dh-hydro/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013/ROUTING/LIS_HIST.d01.zarr"

s3m().upload_files( local_path, s3_path )