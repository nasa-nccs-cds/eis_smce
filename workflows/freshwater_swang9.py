from data import eisc
from data import zc

test_run = False
input_dir = "/discover/nobackup/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013"
bucket = "eis-dh-hydro"
month = "201303" if test_run else  "*"
eisc( cache = "/gpfsm/dnb43/projects/p151/zarr", mode = "eis.freshwater.swang9" )
time_format = "%Y%m%d%H%M"

dsets = [
#    dict(   input = f"file://{input_dir}/ROUTING/{month}/LIS_RST_HYMAP2_router" + "_{time}.d01.nc",
#            output=f"/gpfsm/dnb43/projects/p151/zarr/LIS/OL_1km/SURFACEMODEL/LIS_RST_HYMAP2_router.d01.zarr",
#            time_format = time_format ),

    dict(   input = f"file://{input_dir}/ROUTING/{month}/LIS_HIST" + "_{time}.d01.nc",
            output=f"/gpfsm/dnb43/projects/p151/zarr/LIS/OL_1km/ROUTING/LIS_HIST.d01.zarr",
            time_format = time_format ),

    dict(   input=f"file://{input_dir}/SURFACEMODEL/{month}/LIS_HIST" + "_{time}.d01.nc",
            output=f"/gpfsm/dnb43/projects/p151/zarr/LIS/OL_1km/SURFACEMODEL/LIS_HIST.d01.zarr",
            time_format = time_format ),
#
#    dict(   input=f"file://{input_dir}/SURFACEMODEL/{month}/LIS_RST_NOAHMP401" + "_{time}.d01.nc",
#            output=f"/gpfsm/dnb43/projects/p151/zarr/freshwater.swang.2013/output/SURFACEMODEL/LIS_RST_NOAHMP401.d01.zarr",
#            time_format = time_format ),
]

if __name__ == '__main__':

    zc().standard_conversions( dsets )


