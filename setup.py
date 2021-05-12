from __future__ import print_function
from setuptools import setup, find_packages
import os, shutil, glob

name = 'eis_smce'
LONG_DESCRIPTION = 'Applications and utilities to support the NASA Earth Information System'
cfg_dir = os.path.expanduser("~/.eis_smce/config")
eis_cfg_files = glob.glob( os.path.join( os.path.dirname(os.path.abspath(__file__)), "config", "*.cfg" ) )
version = "0.1"

setup_args = dict(
    name=name,
    version=version,
    description=LONG_DESCRIPTION,
    include_package_data=True,
    install_requires=[  ],
    packages=find_packages(),
    zip_safe=False,
    author='Thomas Maxwell',
    author_email='thomas.maxwell@nasa.gov',
    url='https://github.com/nasa-nccs-cds/eis_smce',
    data_files=[ ],
    entry_points={
        'intake.drivers': [
            'hdf4=eis_smce.data.intake.hdf4.drivers:HDF4Source',
        ]
    },
)

setup(**setup_args)
os.makedirs( cfg_dir, exist_ok=True )
for eis_cfg_file in eis_cfg_files:
    shutil.copy( eis_cfg_file, cfg_dir )



