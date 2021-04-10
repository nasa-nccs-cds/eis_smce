import traitlets.config as tlc
import intake, os, boto3
from intake.catalog.local import YAMLFilesCatalog
from intake_xarray.xzarr import ZarrSource

class CatalogManager(tlc.SingletonConfigurable):

    bucket = tlc.Unicode( "eis-dh-fire" ).tag(config=True)

    def __init__( self, **kwargs ):
        tlc.SingletonConfigurable.__init__( self, **kwargs )
        self.catalog_path: str = kwargs.get( 'cat_path', self.default_catalog_path )
        print( f" Creating YAMLFilesCatalog with path = {self.catalog_path}, kwargs = {kwargs}")
        self._cat: YAMLFilesCatalog = YAMLFilesCatalog( self.catalog_path )


    @property
    def s3(self):
        if self._s3 is None:  self._s3 = boto3.resource('s3')
        return self._s3

    @property
    def default_catalog_path(self) -> str:
        return f"s3://{self.bucket}/catalog"

    def addEntry( self, source: ZarrSource ):
        entry_yml = source.yaml()
        cat_name = source.urlpath.split("/")[-1]
        catalog = f"{self.catalog_path}/{cat_name}.yml"
        print( f"addEntry: Catalog={catalog}, Entry = {entry_yml}" )
        if catalog.startswith("s3:"):  self._s3.Object( self.bucket, catalog ).put( Body=entry_yml )
        else:                          self.write_cat_file( catalog, entry_yml )
        self._cat.reload()

    @property
    def cat(self) -> YAMLFilesCatalog:
        return self._cat

    def write_cat_file(self, catalog: str, entry: str ):
        with open( catalog, "w" ) as fp:
            fp.write( entry )

def cm(**kwargs) -> CatalogManager: return CatalogManager.instance(**kwargs)