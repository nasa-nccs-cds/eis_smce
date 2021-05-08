import time, logging
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from eis_smce.data.common.base import eisc
from eis_smce.data.conversion.zarr import zc
from eis_smce.data.common.cluster import dcm
logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

eisc( cache="/gpfsm/dnb43/projects/p151/zarr", mode="eis.freshwater.swang9", time_format="%Y%m%d%H%M", batch_size=1000, merge_dim='time' )

input_dir = "/discover/nobackup/projects/eis_freshwater"
imerg_fixed_10km = dict(  input=f"file://{input_dir}/swang9/OL_10km/OUTPUT.1980.imerg.fixed/SURFACEMODEL/*/LIS_HIST" + "_{time}.d01.nc",
                        output=f"/gpfsm/dnb43/projects/p151/zarr/LIS/OL_10km/1980/MERRA_IMERG"  )
routing_2013_1km = dict(  input=f"file://discover/nobackup/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013/ROUTING/**/LIS_HIST*.nc",
                          output=f"/gpfsm/dnb43/projects/p151/zarr/LIS/OL_1km/ROUTING/LIS_HIST.d01"  )
surface_2013_1km = dict(  input=f"file://discover/nobackup/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013/SURFACEMODEL/*/LIS_HIST*.nc",
                          output=f"/gpfsm/dnb43/projects/p151/zarr/LIS/OL_1km/SURFACEMODEL/LIS_HIST.d01"  )

if __name__ == '__main__':

    dset = surface_2013_1km
    dcm().init_cluster(processes=True)
    zc().standard_conversion( dset['input'],  dset['output'] )