import intake, time
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from eis_smce.data.intake.zarr.source import EISZarrSource
from eis_smce.data.intake.catalog import cm
from eis_smce.data.common.base import eisc
from eis_smce.data.conversion.zarr import zc
from eis_smce.data.common.cluster import dcm

test_run = False
input_dir = "/discover/nobackup/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013"
name = "freshwater.swang9.OL_1km.2013"
bucket = "eis-dh-hydro"
month = "201303" if test_run else  "*"
s3_prefix = f"projects/eis_freshwater/swang9.OL_1km.2013.new"
eisc( cache = "/discover/nobackup/tpmaxwel/cache", mode = "freshwater.swang9" )

dsets = [
    # dict(   input = f"file://{input_dir}/ROUTING/{month}/LIS_RST_HYMAP2_router" + "_{time}.d01.nc",
    #         output = f"s3://{bucket}/{s3_prefix}/ROUTING/LIS_RST_HYMAP2_router.d01.zarr" ),

    # dict(   input=f"file://{input_dir}/ROUTING/{month}/LIS_HIST" + "_{time}.d01.nc",
    #        output=f"s3://{bucket}/{s3_prefix}/ROUTING/LIS_HIST.d01.zarr"  ),

    dict(   input=f"file://{input_dir}/SURFACEMODEL/{month}/LIS_HIST" + "_{time}.d01.nc",
            output=f"s3://{bucket}/{s3_prefix}/SURFACEMODEL/LIS_HIST.d01.zarr" ),

 #   dict(   input=f"file://{input_dir}/SURFACEMODEL/{month}/LIS_RST_NOAHMP401" + "_{time}.d01.nc",
 #           output=f"s3://{bucket}/{s3_prefix}/SURFACEMODEL/LIS_RST_NOAHMP401.d01.zarr"  ),
 #           output=f"file:///discover/nobackup/tpmaxwel/cache/eis_freshwater.2"  ),
]

if __name__ == '__main__':

#    dcm().init_cluster( processes=False )
    sources: List[EISZarrSource] = zc().standard_conversions( dsets, merge_dim="time"  )
    cm().add_entries( bucket, sources, name )


