import intake, time
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import xarray as xa
from eis_smce.data.common.base import eisc
from eis_smce.data.conversion.zarr import zc
from eis_smce.data.common.cluster import dcm

test_run = True
input_dir = "/discover/nobackup/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013"
month = "201303" if test_run else  "*"
eisc( cache = "/gpfsm/dnb43/projects/p151/zarr", mode = "eis.freshwater.swang9" )
input=f"/{input_dir}/SURFACEMODEL/{month}/LIS_RST_NOAHMP401" + "_{time}.d01.nc"

if __name__ == '__main__':

    dcm().init_cluster()
    dset :xa.Dataset = zc().get_input( input, merge_dim="time" )

    print( dset )
    print( f"dset.attrs = {dset.attrs}\n" )
    print( f"dset.vars = {vars(dset)}\n" )

    vid, first_var = list(dset.items())[0]

    print(first_var)
    print(f"\n{vid}.attrs = {first_var.attrs}\n")
    print(f"\n{vid}.vars = {vars(first_var)}\n")


