import logging, sys
from data import  eisc_config
from data import zc
from data import dcm
logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

if len(sys.argv) == 1:
    print( f"Usage: >> python {sys.argv[0]} <config_file_path>")
    sys.exit(-1)

eisc = eisc_config( sys.argv[1] )

if __name__ == '__main__':

    dcm().init_cluster( processes=True )

    input_path  = f"{eisc['input_dir']}/{eisc['input_dset']}"
    output_path = f"{eisc['output_dir']}/{eisc['output_dset']}"
    zarr_url   = f"s3://{eisc['bucket']}/{eisc['output_dset']}"
    cat_name = eisc.get( 'cat_name', eisc['output_dset'].replace("/",".") )

    zc().standard_conversion( input_path, output_path  )
    zc().write_catalog( f"{output_path}.zarr", cat_name )

    print( f"S3 upload command:\n\t '>> aws s3 mv {output_path}.zarr {zarr_url}.zarr  --acl bucket-owner-full-control --recursive' ")

    dcm().shutdown()
    sys.exit(0)