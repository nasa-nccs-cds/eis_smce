import intake

input_dir = "/discover/nobackup/projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013"
bucket = "eis-dh-hydro"
month = "201302"
s3_prefix = f"projects/eis_freshwater/swang9/OL_1km/OUTPUT.RST.2013"

dsets = [
    dict(   input = f"file://{input_dir}/ROUTING/{month}/LIS_RST_HYMAP2_router" + "_{time}.d01.nc",
            output = f"s3://{bucket}/{s3_prefix}/ROUTING/IS_RST_HYMAP2_router.d01.zarr",
            merge_dim = "time" ),

    dict(   input=f"file://{input_dir}/ROUTING/{month}/LIS_HIST" + "_{time}.d01.nc",
            output=f"s3://{bucket}/{s3_prefix}/ROUTING/LIS_HIST.d01.zarr",
            merge_dim = "time" ),

    dict(   input=f"file://{input_dir}/SURFACEMODEL/{month}/LIS_HIST" + "_{time}.d01.nc",
            output=f"s3://{bucket}/{s3_prefix}/SURFACEMODEL/LIS_HIST.d01.zarr",
            merge_dim = "time" ),

    dict(   input=f"file://{input_dir}/SURFACEMODEL/{month}/LIS_RST_NOAHMP401" + "_{time}.d01.nc",
            output=f"s3://{bucket}/{s3_prefix}/SURFACEMODEL/LIS_RST_NOAHMP401.d01.zarr",
            merge_dim = "time" ),
]

if __name__ == '__main__':

    for dset in dsets:
        [input, output] = [dset.pop(key) for key in [ 'input', 'output' ] ]
        h4s = intake.open_hdf4( input )
        h4s.export( output, bucket=bucket, **dset )


