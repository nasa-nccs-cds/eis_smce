from __future__ import print_function
from setuptools import setup, find_packages

name = 'eis_smce'
LONG_DESCRIPTION = 'Applications and utilities to support the NASA Earth Information System'
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
)

setup(**setup_args)



