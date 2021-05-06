import glob, xarray as xa

input_path = "/discover/nobackup/projects/eis_freshwater/lahmers/RUN/1km_DOMAIN_DAens20_MCD15A2H.006_2019Flood/OUTPUT/SURFACEMODEL/*/LIS_HIST*.nc"
input_files = glob.glob( input_path )
merge_dim = "time"
cache_dir = "/discover/nobackup/tpmaxwel/cache"
zarr_file = f"{cache_dir}/zarr_test.zarr"

dset: xa.Dataset = xa.open_mfdataset( input_files, concat_dim=merge_dim,  parallel = True ) # coords="minimal",
dset.to_zarr( zarr_file, mode="w" )