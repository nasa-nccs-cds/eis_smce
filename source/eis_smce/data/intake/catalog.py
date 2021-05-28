from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import intake, os, boto3
from eis_smce.data.common.base import EISSingleton
import yaml, xarray as xr
from intake.catalog.local import YAMLFilesCatalog, YAMLFileCatalog
from intake.interface.gui import GUI
from eis_smce.data.intake.zarr.source import EISZarrSource

class CatalogManager(EISSingleton):

    bucket = tlc.Unicode( "eis-dh-fire" ).tag(config=True)

    def __init__( self, **kwargs ):
        EISSingleton.__init__( self, **kwargs )
        self._s3 = None

    @property
    def s3(self):
        if self._s3 is None:  self._s3 = boto3.resource('s3')
        return self._s3

    def cat_path( self, bucket: str, cat_name = "*" ) -> str:
        return f"s3://{bucket}/catalog/{cat_name}.yml"

    def gui( self, bucket: str ):
        bucket = self.s3.Bucket(bucket)
        catalogs = [ f"s3://{obj.bucket_name}/{obj.key}" for obj in bucket.objects.filter(Prefix="catalog") ]
        self.logger.info( f"Opening gui with catalogs: {catalogs}")
        return GUI( catalogs )

    def cat( self, bucket: str, name: str = None ) -> YAMLFileCatalog:
        cat_path = self.cat_path(bucket,name)
        self.logger.debug(f"Open YAMLFileCatalog from url: {cat_path}")
        return YAMLFileCatalog( cat_path )

    def mcat( self, bucket: str ) -> YAMLFilesCatalog:
        cat_path = self.cat_path(bucket)
        self.logger.debug(f"Open YAMLFilesCatalog from url: {cat_path}")
        return YAMLFilesCatalog( cat_path )

    def add_entries( self, bucket: str, sources: List[EISZarrSource], name = None, **kwargs ):
        from eis_smce.data.common.base import eisc
        if name is None:
            for source in sources:
                prefix = f"catalog/{source.cat_name}.yml"
                self.s3.Object( bucket, prefix ).put( Body=source.yaml(**kwargs) , ACL="bucket-owner-full-control" )
        else:
            catalog: YAMLFileCatalog = self.cat( bucket, name )
            for source in sources:
                catalog.add( source, source.cat_name )
            prefix = f"catalog/{name}.yml"
            self.s3.Object( bucket, prefix ).put( Body=catalog.yaml(), ACL="bucket-owner-full-control" )

        eisc().save_config()


    def delete_entry( self, bucket: str, cat_name: str ):
        from eis_smce.data.storage.s3 import s3m
        catpath = self.cat_path( bucket, cat_name )
        s3m().delete( catpath )

    def clear(self):
        self._s3 = None

def cm() -> CatalogManager: return CatalogManager.instance()