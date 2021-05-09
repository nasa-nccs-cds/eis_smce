import time, logging
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from eis_smce.data.common.base import eisc
from eis_smce.data.conversion.zarr import zc
from eis_smce.data.common.cluster import dcm
logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

partial_run = True
eisc( cache="/gpfsm/dnb43/projects/p151/zarr", mode="eis.freshwater.swang9", time_format="%Y%m%d%H%M", batch_size=1000, merge_dim='time' )

month = "200304" if partial_run else "**"
input_dir = "/discover/nobackup/projects/eis_freshwater"
output_dir = "/gpfsm/dnb43/projects/p151/zarr"
imerg_fixed_10km = dict(  input=f"file://{input_dir}/swang9/OL_10km/OUTPUT.1980.imerg.fixed/SURFACEMODEL/{month}/LIS_HIST" + "_{time}.d01.nc",
                        output=f"{output_dir}/LIS/OL_10km/1980/MERRA_IMERG"  )
routing_2013_1km = dict(  input=f"file:/{input_dir}/swang9/OL_1km/OUTPUT.RST.2013/ROUTING/{month}/LIS_HIST*.nc",
                          output=f"{output_dir}/LIS/OL_1km/ROUTING/LIS_HIST.d01"  )
merra_2000_1km = dict(  input=f"file:/{input_dir}/swang9/OL_1km/OUTPUT.RST.2000/SURFACEMODEL/{month}/LIS_HIST*.nc",
                        output=f"{output_dir}/LIS/OL_1km/2000_2021/MERRA/LIS_HIST.d01"  )
merra_imerg_2000_1km = dict(  input=f"file:/{input_dir}/swang9/OL_1km/OUTPUT.RST.2000.imerg.fixed/SURFACEMODEL/{month}/LIS_HIST*.nc",
                              output=f"{output_dir}/LIS/OL_1km/2000_2021/MERRA_IMERG/LIS_HIST.d01" )

if __name__ == '__main__':

    dcm().init_cluster( processes=True )

    dset = merra_imerg_2000_1km
    zc().standard_conversion( dset['input'],  dset['output'] )

    dcm().shutdown()