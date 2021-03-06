import logging
from data import eisc
from data import zc
from data import dcm
logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)

partial_run = False
eisc( cache="/gpfsm/dnb43/projects/p151/zarr", mode="eis.freshwater.swang9", time_format="%Y%m%d%H%M", batch_size=1000, merge_dim='time' )
bucket = "eis-dh-hydro"

month = "200304" if partial_run else "**"
input_dir = "/discover/nobackup/projects/eis_freshwater"
output_dir = "/gpfsm/dnb43/projects/p151/zarr"
imerg_fixed_10km       = dict(  input=f"swang9/OL_10km/OUTPUT.1980.imerg.fixed/SURFACEMODEL/{month}/LIS_HIST" + "_{time}.d01.nc",
                                output=f"LIS/OL_10km/1980/MERRA_IMERG"  )
routing_2013_1km       = dict(  input=f"swang9/OL_1km/OUTPUT.RST.2013/ROUTING/{month}/LIS_HIST*.nc",
                                output=f"LIS/OL_1km/ROUTING/LIS_HIST.d01"  )
merra_2000_1km         = dict(  input=f"swang9/OL_1km/OUTPUT.RST.2000/SURFACEMODEL/{month}/LIS_HIST*.nc",
                                output=f"LIS/OL_1km/2000_2021/MERRA/LIS_HIST.d01"  )
merra_imerg_2000_1km   = dict(  input=f"swang9/OL_1km/OUTPUT.RST.2000.imerg.fixed/SURFACEMODEL/{month}/LIS_HIST*.nc",
                                output=f"LIS/OL_1km/2000_2021/MERRA_IMERG/LIS_HIST.d01" )
MCD15A2H_2019Flood     = dict(  input=f"lahmers/RUN/1km_DOMAIN_DAens20_MCD15A2H.006_2019Flood/OUTPUT/ROUTING/**/LIS_HIST*.nc",
                                output=f"LIS/DA_1km/MODIS_Flood_2019/ROUTING/LIS_HIST.d01" )
MCD15A2H_2019FloodSM   = dict(  input=f"lahmers/RUN/1km_DOMAIN_DAens20_MCD15A2H.006_2019Flood/OUTPUT/SURFACEMODEL/**/LIS_HIST*.nc",
                                output=f"LIS/DA_1km/MODIS_Flood_2019/SURFACEMODEL/LIS_HIST.d01" )
MCD15A2H_2019Drought   = dict(  input=f"/discover/nobackup/projects/eis_freshwater/lahmers/RUN/1km_DOMAIN_DAens20_MCD15A2H.006_2019Drought/OUTPUT/SURFACEMODEL/**/LIS_HIST*.nc",
                                output=f"LIS/DA_1km/MODIS_Drought_2019/SURFACEMODEL/LIS_HIST.d01" )
MCD15A2H_2002Drought   = dict(  input=f"ahmers/RUN/10km_DOMAIN_DAens20_MCD15A2H.006_2002_2021/OUTPUT.DA/SURFACEMODEL/**/LIS_HIST*.nc",    # Needs fixing.
                                output=f"LIS/DA_10km/MODIS_Droughts_2002_2021/SURFACEMODEL/LIS_HIST.d01" )
AMSR2                  = dict(  input=f"mwrzesie/Runs_10km/DA_AMSR2/OUTPUT_DA_AMSR2/SURFACEMODEL/**/LIS_HIST*.nc",
                                output=f"LIS/DA_10km/AMSR2/SURFACEMODEL/LIS_HIST.d01" )
SNODAS                 = dict(  input=f"mwrzesie/Runs_10km/DA_SNODAS/OUTPUT_snodasDA_obserr5cm/SURFACEMODEL/**/LIS_HIST*.nc",
                                output=f"LIS/DA_10km/SNODAS/SURFACEMODEL/LIS_HIST.d01" )
MCD15A2H_wSmoothing    = dict(  input=f"lahmers/RUN/10km_DOMAIN_DAens20_MCD15A2H.wSmoothing.006_2002_2021/OUTPUT.LAIsmooth.DA/SURFACEMODEL/**/LIS_HIST*.nc",
                                output=f"LIS/DA_10km/MODIS_wSoother_2002_2021/SURFACEMODEL/LIS_HIST.d01" )
THYSM                  = dict(  input=f"pliu/WORKING/DA_THYSM/lis_run/OUTPUT_010d/SURFACEMODEL/**/LIS_HIST*.nc",
                                output=f"LIS/DA_010d/THYSM/SURFACEMODEL/LIS_HIST.d01" )

dset = MCD15A2H_2019FloodSM

if __name__ == '__main__':

    dcm().init_cluster( processes=True )

    zc().standard_conversion( f"file:/{input_dir}/{dset['input']}",  f"file:/{output_dir}/{dset['output']}" )
    print( f"S3 upload command:\n\t '>> aws s3 mv {output_dir}/{dset['output']}.zarr   s3://{bucket}/{dset['output']}.zarr  --acl bucket-owner-full-control --recursive' ")

    dcm().shutdown()