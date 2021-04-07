import traitlets.config as tlc
import intake, os, boto3
from intake.catalog.local import YAMLFilesCatalog
from intake.source.base import DataSource

class CatalogManager(tlc.SingletonConfigurable):

    bucket = tlc.Unicode( "eis-dh-fire" ).tag(config=True)

    def __init__( self, **kwargs ):
        tlc.SingletonConfigurable.__init__( self, **kwargs )
        self._cat: YAMLFilesCatalog = YAMLFilesCatalog()
        self._s3 = boto3.resource('s3')

    @property
    def catalog_url(self) -> str:
        return f"s3://{self.bucket}/catalog"

    def addEntry( self, name: str, source: DataSource ):
        entry_yml = source.yaml()
        print( f"Add Entry to Catalog: {entry_yml}" )
        self._s3.Object( self.bucket, f"catalog/{name}.yml" ).put( Body=entry_yml )
        self._cat.reload()

    @property
    def cat(self) -> YAMLFilesCatalog:
        return self._cat

def cm() -> CatalogManager: return CatalogManager.instance()