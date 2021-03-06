import time, logging, sys
from eis_smce.data.common.base import  eisc_config
from eis_smce.data.conversion.zarr import zc
from eis_smce.data.common.cluster import dcm
logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

if len(sys.argv) == 1:
    print( f"Usage: >> python {sys.argv[0]} <config_file_path>")
    sys.exit(-1)

eisc = eisc_config( sys.argv[1] )

if __name__ == '__main__':

    dcm().init_cluster( processes=True )

    input_path  = f"{eisc['input_dir']}/{eisc['input_dset']}"
    output_path =   f"{eisc['output_dir']}/{eisc['output_dset']}"
    zarr_url   = f"s3://{eisc['bucket']}/{eisc['output_dset']}"
    cat_name = eisc.get( 'cat_name', eisc['output_dset'].replace("/",".") )
    file_index_range = eisc.get( 'file_index_range', None )

    zc().standard_conversion( input_path, output_path, file_index_range = file_index_range )
    zc().write_catalog( f"{output_path}.zarr", cat_name )

    print( f"S3 upload command:\n\t '>> aws s3 mv {output_path}.zarr {zarr_url}.zarr  --acl bucket-owner-full-control --recursive' ")

    dcm().shutdown()
    sys.exit(0)