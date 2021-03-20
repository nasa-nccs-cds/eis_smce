# eis_smce
Application and utilities to support the NASA Earth Information System

Conda Setup
---------------
Create your conda environment as follows:

    > conda create --name eis_smce
    > conda activate eis_smce
    > conda install -c conda-forge xarray numpy boto3 dask pyhdf zarr        # s3fs 



AWS Configuration
---------------
Before using eis_smce, you need to set up authentication credentials for your AWS account using either the [IAM Console](https://console.aws.amazon.com/iam/home) 
or the AWS CLI. You can either choose an existing user or create a new one.
For instructions about how to create a user using the IAM Console, 
see [Creating IAM users](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html#id_users_create_console). 
Once the user has been created, see [Managing access keys](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey) 
to learn how to create and retrieve the keys used to authenticate the user.
If you have the [AWS CLI](http://aws.amazon.com/cli/) installed, then you can use the aws configure command to configure your credentials file:

  > aws configure

Alternatively, you can create the credentials file yourself. By default, its location is *~/.aws/credentials*. 
At a minimum, the credentials file should specify the access key and secret access key. In this example, the key and secret key for the account are specified the *default* profile:
```
[default]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
```
You may also want to add a default region to the AWS configuration file, which is located by default at *~/.aws/config*:
```
[default]
region=us-east-1
```
Alternatively, you can pass a *region_name* when creating clients and resources.
You have now configured credentials for the default profile as well as a default region to use when creating connections. 
See [Configuration](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#guide-configuration) 
for in-depth configuration sources and options.

