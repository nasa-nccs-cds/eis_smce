import time, logging
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from eis_smce.data.common.base import eisc
from eis_smce.data.conversion.zarr import zc
from eis_smce.data.common.cluster import dcm
logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

test_run = False
input_dir = "/discover/nobackup/projects/eis_freshwater/swang9/OL_10km/OUTPUT.1980.imerg.fixed"
bucket = "eis-dh-hydro"
month = "199312" if test_run else  "*"

if __name__ == '__main__':

    dcm().init_cluster()
    eisc(cache="/gpfsm/dnb43/projects/p151/zarr", mode="eis.freshwater.swang9", time_format="%Y%m%d%H%M")
    input=f"file://{input_dir}/SURFACEMODEL/{month}/LIS_HIST" + "_{time}.d01.nc",
    output=f"/gpfsm/dnb43/projects/p151/zarr/LIS/OL_10km/2000_2021/MERRA_IMERG.zarr",
    zc().standard_conversion( input, output )