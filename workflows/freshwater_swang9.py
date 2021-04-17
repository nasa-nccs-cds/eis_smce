import intake, time
from eis_smce.data.intake.zarr.source import EISZarrSource
from eis_smce.data.intake.catalog import cm
from eis_smce.data.common.base import eisc

test_run = False
input_dir = "/discover/nobackup/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013"
name = "freshwater.swang9.OL_1km.2013"
bucket = "eis-dh-hydro"
month = "201303" if test_run else  "*"
s3_prefix1 = f"projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013"
s3_prefix =  f"projects/eis_freshwater/swang9.OL_1km.2013"
eisc( cache = "/discover/nobackup/tpmaxwel/cache", mode = "freshwater.swang9" )

dsets = [
    # dict(   input = f"file://{input_dir}/ROUTING/{month}/LIS_RST_HYMAP2_router" + "_{time}.d01.nc",
    #         output = f"s3://{bucket}/{s3_prefix}/ROUTING/LIS_RST_HYMAP2_router.d01.zarr" ),

    # dict(   input=f"file://{input_dir}/ROUTING/{month}/LIS_HIST" + "_{time}.d01.nc",
    #         output=f"s3://{bucket}/{s3_prefix}/ROUTING/LIS_HIST.d01.zarr"  ),

    dict(   input=f"file://{input_dir}/SURFACEMODEL/{month}/LIS_HIST" + "_{time}.d01.nc",
            output=f"s3://{bucket}/{s3_prefix}/SURFACEMODEL/LIS_HIST.d01.zarr" ),

    # dict(   input=f"file://{input_dir}/SURFACEMODEL/{month}/LIS_RST_NOAHMP401" + "_{time}.d01.nc",
    #         output=f"s3://{bucket}/{s3_prefix}/SURFACEMODEL/LIS_RST_NOAHMP401.d01.zarr"  ),
]

if __name__ == '__main__':

    for dset in dsets:
        t0 = time.time()
        [input, output] = [dset.pop(key) for key in [ 'input', 'output' ] ]
        h4s = intake.open_hdf4( input )
        zs: EISZarrSource = h4s.export( output, bucket=bucket, merge_dim = "time", **dset )
        cm().addEntry( zs  )
        if zs: print( f"Completed {zs.cat_name} conversion & upload to {output} in {(time.time()-t0)/60} minutes" )

    cm().write_s3( bucket, name )


