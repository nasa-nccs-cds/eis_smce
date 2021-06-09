import logging
from data import eisc
from data import zc
from data import dcm
logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

partial_run = False
input_dir = "/discover/nobackup/projects/eis_freshwater/swang9/OL_10km/OUTPUT.1980.imerg.fixed"
month = "200*" if partial_run else  "*"

if __name__ == '__main__':

    eisc(cache="/gpfsm/dnb43/projects/p151/zarr", mode="eis.freshwater.swang9", time_format="%Y%m%d%H%M")
    input=f"file://{input_dir}/SURFACEMODEL/{month}/LIS_HIST" + "_{time}.d01.nc"
    output=f"/gpfsm/dnb43/projects/p151/zarr/LIS/OL_10km/1980/MERRA_IMERG"

    dcm().init_cluster(processes=True)
    zc().standard_conversion( input, output )