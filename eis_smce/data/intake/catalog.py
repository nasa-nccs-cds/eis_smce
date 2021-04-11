import traitlets.config as tlc
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import intake, os, boto3
import yaml, xarray as xr
from intake.catalog.local import YAMLFilesCatalog
from eis_smce.data.intake.zarr.source import EISZarrSource

class CatalogManager(tlc.SingletonConfigurable):

    bucket = tlc.Unicode( "eis-dh-fire" ).tag(config=True)

    def __init__( self, **kwargs ):
        tlc.SingletonConfigurable.__init__( self, **kwargs )
        self._s3 = None
        new_bucket = kwargs.get( 'bucket', None )
        if new_bucket: self.bucket = new_bucket
        self.file_path: str = kwargs.get( 'file_path', None )

    @property
    def s3(self):
        if self._s3 is None:  self._s3 = boto3.resource('s3')
        return self._s3

    @property
    def s3_cat_path(self) -> str:
        return f"s3://{self.bucket}/catalog"

    @property
    def cat_path(self) -> str:
        return self.file_path or self.s3_cat_path

    def cat( self ) -> intake.Catalog:
        cat_path = f"{self.cat_path}/*.yml"
        return intake.open_catalog( cat_path )

    def addEntry( self, source: EISZarrSource, **kwargs ):
        entry_yml = source.yaml( **kwargs )
        if self.file_path is None:
            catalog = f"catalog/{source.cat_name}.yml"
            self.s3.Object( self.bucket, catalog ).put( Body=entry_yml )
        else:
            catalog = f"{self.file_path}/{source.cat_name}.yml"
            self.write_cat_file( catalog, entry_yml )
        print(f"addEntry: Catalog={catalog}, Entry = {entry_yml}")

    def write_cat_file(self, catalog: str, entry: str ):
        with open( catalog, "w" ) as fp:
            fp.write( entry )

def cm(**kwargs) -> CatalogManager: return CatalogManager.instance(**kwargs)