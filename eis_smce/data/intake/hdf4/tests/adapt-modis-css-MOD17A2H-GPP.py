import os, xarray as xr
import intake

base_dir = "/css/modis/Collection6/L3/"
cache_dir = "/att/nobackup/tpmaxwel/ILAB/scratch"
collection = "MOD17A2H-GPP"
year = "2004"
day = "0*"
file_names = "MOD17A2H.{sample}.h09v09.006.{sid}.hdf"
batch = f"{year}/{day}/{file_names}"
output_file = f"{cache_dir}/{collection}/h09v09.zarr"
os.makedirs( os.path.dirname(output_file), exist_ok=True )
data_url = f"file://{base_dir}/{collection}/{batch}"

if __name__ == '__main__':

    h4s = intake.open_hdf4( data_url )
    h4s.export( output_file )
    zs = intake.open_zarr( output_file )

    print( "\nZarrSource:" )
    dset: xr.Dataset = zs.to_dask()
    print( dset )

    print( "\n --> Dataset Attributes:" )
    for k,v in dset.attrs.items():
        print( f"   ... {k} = {v}" )


