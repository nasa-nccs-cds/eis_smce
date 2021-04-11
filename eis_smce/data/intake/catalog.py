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

    @property
    def s3(self):
        if self._s3 is None:  self._s3 = boto3.resource('s3')
        return self._s3

    def cat_path( self, bucket: str ) -> str:
        return f"s3://{bucket}/catalog/*.yml"

    def cat( self, bucket: str ) -> intake.Catalog:
        cat_path = self.cat_path(bucket)
        print( f"Open catalog from url: {cat_path}")
        return intake.open_catalog(  cat_path )

    def addEntry( self, source: EISZarrSource, **kwargs ):
        entry_yml = source.yaml( **kwargs )
        file_path = kwargs.get( 'path', None )
        if file_path is None:
            bucket = kwargs.get('bucket', None)
            assert bucket is not None, "Must supply the 'bucket' argument when adding an entry to an s3 catalog"
            catalog = f"catalog/{source.cat_name}.yml"
            self.s3.Object( bucket, catalog ).put( Body=entry_yml )
        else:
            catalog = f"{file_path}/{source.cat_name}.yml"
            self.write_cat_file( catalog, entry_yml )
        print(f"addEntry: Catalog={catalog}, Entry = {entry_yml}")

    def write_cat_file(self, catalog: str, entry: str ):
        with open( catalog, "w" ) as fp:
            fp.write( entry )

def cm() -> CatalogManager: return CatalogManager.instance()