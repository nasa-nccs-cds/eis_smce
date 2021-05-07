import glob, time, xarray as xa

input_path0 = "/discover/nobackup/projects/eis_freshwater/lahmers/RUN/1km_DOMAIN_DAens20_MCD15A2H.006_2019Flood/OUTPUT/SURFACEMODEL/*/LIS_HIST*.nc"
input_path = "/discover/nobackup/projects/eis_freshwater/swang9/OL_10km/OUTPUT.1980.imerg.fixed/SURFACEMODEL/*/LIS_HIST*.nc"
input_files = glob.glob( input_path )
merge_dim = "time"
cache_dir = "/discover/nobackup/tpmaxwel/cache"
zarr_file = f"{cache_dir}/imerg.fixed.test.zarr"

t0 = time.time()
print( f"Opening dataset glob: {input_path}")
dset: xa.Dataset = xa.open_mfdataset( input_files, concat_dim=merge_dim,  parallel = True ) # coords="minimal",
print( f"Converting to zarr file: {zarr_file}")
dset.to_zarr( zarr_file, mode="w" )
print( f"Done in {(time.time()-t0)/60} minutes")