# eis_smce
Applications and utilities to support the NASA Earth Information System

Conda Setup
---------------
Create your conda environment as follows:

    > conda create --name eis_smce 
    > conda activate eis_smce
    > conda install -c conda-forge -c pyviz/label/dev awscli boto3 dask hvplot intake intake-xarray ipykernel jupyterlab numpy panel pyhdf pyviz rasterio rioxarray s3fs traitlets xarray zarr 
    > jupyter labextension install @pyviz/jupyterlab_pyviz
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

* Setup Configuration (enter access keys created above):

    > aws configure

* Setup MFA (must be done daily):
    From the base directory of this project, execute:
  
    > source ./config/mfa.sh







