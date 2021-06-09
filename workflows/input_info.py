import xarray as xa
from data import eisc
from data import zc
from data import dcm

test_run = True
input_dir = "/discover/nobackup/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013"
month = "201303" if test_run else  "*"
eisc( cache = "/gpfsm/dnb43/projects/p151/zarr", mode = "eis.freshwater.swang9" )
input = f"{input_dir}/SURFACEMODEL/{month}/LIS_HIST" + "_{time}.d01.nc"

if __name__ == '__main__':

    dcm().init_cluster()
    dset: xa.Dataset = zc().get_input( input, merge_dim="time" )

    time = dset['time'].values
    path = dset['_eis_source_path'].values
    for idx in range(time.size):
       print( f"{time[idx]}: {path[idx]}")




