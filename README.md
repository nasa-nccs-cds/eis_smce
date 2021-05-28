# eis_smce
Applications and utilities to support the NASA Earth Information System

Conda Setup
---------------
Create your conda environment as follows:

    > conda create --name eis_smce 
    > conda activate eis_smce
    > conda install -c conda-forge awscli boto3 dask hvplot intake intake-xarray  numpy pyhdf rasterio rioxarray s3fs traitlets xarray zarr dask-jobqueue 

eis_smce Setup
---------------
Install eis_smce as follows:

    > cd <install_dir>
    > git clone https://github.com/nasa-nccs-cds/eis_smce.git 
    > cp ~/eis_smce/config/*.cfg <your_config_dir>

eis_smce Configuration
----------------------

* See the config [README](./config/README.md) for configuration documentation.
* Edit the *<your_config_dir>/zc.cfg* file to configure zarr conversion.

Zarr Conversion
---------------
To execute the zarr conversion workflow:

    > cd <install_dir>/eis_smce/workflows
    > python zarr_conversion.py <your_config_dir>/zc.cfg

Setup Amazon Credentials
------------------------

* Create Access Keys:  https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey

* Setup Configuration (enter access keys created above):

    > aws configure

* Setup MFA (must be done daily):
    From the base directory of this project, execute:
  
    > source ./config/mfa.sh
   

Discover Notes
--------------

### Compute 

>> salloc --nodes=1 --constraint="sky|hasw" --time=12:00:00
>> conda activate eis_smce
>> cd <install_dir>/eis_smce
>> python ./workflows/zarr_conversion.py ~/.eis_smce/config/zc.cfg

### Upload

>> salloc -p datamove --nodes=1 --time=02:00:00
>> module load aws
>> cd <install_dir>/eis_smce
>> source ./config/mfa.sh
>> aws s3 mv  <local_zarr_path>  <s3_zarr_path> --acl bucket-owner-full-control --recursive

#### Example CLI upload command:

>> aws s3 mv  /gpfsm/dnb43/projects/p151/zarr/LIS/OL_1km/SURFACEMODEL/LIS_HIST.d01.zarr  s3://eis-dh-hydro/LIS/OL_1km/SURFACEMODEL/LIS_HIST.d01.zarr --acl bucket-owner-full-control --recursive







