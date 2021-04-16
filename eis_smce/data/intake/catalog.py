import traitlets.config as tlc
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import intake, os, boto3
import yaml, xarray as xr
from intake.catalog.local import YAMLFilesCatalog
from intake.interface.gui import GUI
from eis_smce.data.intake.zarr.source import EISZarrSource

class CatalogManager(EISSingleton):

    bucket = tlc.Unicode( "eis-dh-fire" ).tag(config=True)

    def __init__( self, **kwargs ):
        EISSingleton.__init__( self, **kwargs )
        self._s3 = None
        self._sources = [ "sources:" ]

    @property
    def s3(self):
        if self._s3 is None:  self._s3 = boto3.resource('s3')
        return self._s3


    # def download( self, bucketName, cache_dir: str ) -> str:
    #     s3_resource = boto3.resource('s3')
    #     bucket = s3_resource.Bucket(bucketName)
    #     for obj in bucket.objects.filter(Prefix="catalog"):
    #         obj_dir = f"{cache_dir}/catalog/{os.path.dirname(obj.key)}"
    #         if not os.path.exists(obj_dir): os.makedirs(obj_dir)
    #         bucket.download_file( obj.key, f"{obj_dir}/{os.path.basename(obj.key)}" )
    #     return

    def cat_path( self, bucket: str ) -> str:
        return f"s3://{bucket}/catalog/*.yml"

    def gui( self, bucket: str ):
        bucket = self.s3.Bucket(bucket)
        catalogs = [ f"s3://{obj.bucket_name}/{obj.key}" for obj in bucket.objects.filter(Prefix="catalog") ]
        print( f"Opening gui with catalogs: {catalogs}")
        return GUI( catalogs )

    def cat( self, bucket: str ) -> intake.Catalog:
        cat_path = self.cat_path(bucket)
        print( f"Open catalog from url: {cat_path}")
        return intake.open_catalog(  cat_path )

    def addEntry( self, source: EISZarrSource, **kwargs ):
        source_yml = source.yaml( **kwargs )
        self._sources.append( source_yml )

    def get_cat_yml( self ):
        return "\n".join( self._sources )

    def write_s3( self, bucket: str, cat_name: str ):
        catalog = f"catalog/{cat_name}.yml"
        self.s3.Object( bucket, catalog ).put( Body=self.get_cat_yml() , ACL="bucket-owner-full-control" )

    def write_local(self, path: str, cat_name: str):
        catalog = f"{path}/{cat_name}.yml"
        with open(catalog, "w") as fp:
            fp.write( self.get_cat_yml() )

    def clear(self):
        self._s3 = None
        self._sources = [ "sources:" ]

def cm() -> CatalogManager: return CatalogManager.instance()