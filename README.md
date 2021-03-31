# eis_smce
Applications and utilities to support the NASA Earth Information System

Conda Setup
---------------
Create your conda environment as follows:

    > conda create --name eis_smce 
    > conda activate eis_smce
    > conda install -c conda-forge rioxarray rasterio xarray numpy boto3 dask pyhdf zarr traitlets s3fs intake intake-xarray ipykernel
    > python -m ipykernel install --user --name=eis_smce

eis_smce Setup
---------------
Install eis_smce as follows:

    > git clone https://github.com/nasa-nccs-cds/eis_smce.git 
    > cd eis_smce
    > python setup.py install

Setup Amazon Credentials
------------------------

* Create Access Keys:  https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey

* Install CLI:  https://docs.aws.amazon.com/cli/latest/userguide/install-linux.html

    > pip install awscli --upgrade --user

* Setup Configuration:

    > aws configure
    >
    > source ./config/mfa.sh







