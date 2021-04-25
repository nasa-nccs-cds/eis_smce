import time
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from eis_smce.data.common.base import eisc
from eis_smce.data.conversion.zarr import zc
from eis_smce.data.common.cluster import dcm

test_run = False
input_dir = "/discover/nobackup/projects/eis_freshwater/swang9/OL_1km/"
bucket = "eis-dh-hydro"
month = "201303" if test_run else  "*"
eisc( cache = "/gpfsm/dnb43/projects/p151/zarr", mode = "eis.freshwater.swang9" )

dsets = [
    dict(   input = f"file://{input_dir}/lis_input.01.noahmp401.step2_igbp_no_wetland_inner_ocean_char.nc",
            output=f"/gpfsm/dnb43/projects/p151/zarr/freshwater.swang.2013/output/lis_input.01.noahmp401.step2_igbp_no_wetland_inner_ocean_char.zarr" ),
]

if __name__ == '__main__':

    dcm().init_cluster( )
    zc().standard_conversions( dsets )
