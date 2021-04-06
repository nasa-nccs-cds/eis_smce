import traitlets.config as tlc
import intake, os
from intake.catalog.local import YAMLFileCatalog
from intake.source.base import DataSource

class CatalogManager(tlc.SingletonConfigurable):

    def __init__( self, **kwargs ):
        tlc.SingletonConfigurable.__init__( self, **kwargs )
        self._base_catalog_location = kwargs.get( 'catalog', os.path.expanduser("~/.eis_smce/catalog.yaml") )
        self._cat: YAMLFileCatalog = YAMLFileCatalog( self._base_catalog_location )

    def addEntry( self, source: DataSource ):
        entry_yml = source.yaml()
        print( f"Add Entry to Catalog: {entry_yml}" )
#        self._cat.walk()

def cm() -> CatalogManager: return CatalogManager.instance()