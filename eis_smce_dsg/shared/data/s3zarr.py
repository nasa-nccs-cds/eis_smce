import io, os, sys
from pyhdf.SD import SD, SDC
from collections.abc import MutableMapping
from botocore.client import BaseClient
import zarr

class zarrModisDS:

    def __init__( self, client: BaseClient, store: MutableMapping, cache_dir: str  ):
        self._store: MutableMapping = store
        self._client: BaseClient = client
        self._cache_dir = cache_dir

    def from_s3(self, bucketname: str, itemname: str ):
        file_name = itemname.split("/")[-1]
        modis_filepath = os.path.join( self._cache_dir, file_name )
        self._client.download_file(bucketname, itemname, modis_filepath)
        modis_sd: SD = SD(modis_filepath, SDC.READ)
        root = zarr.group(store=self._store)

        for (akey, aval) in modis_sd.attributes().items():
            if akey.startswith("CoreMetadata"):
                pass
            else:
                root.attrs[akey] = aval

        for dskey, dsinfo in modis_sd.datasets().items():
            modis_sds = modis_sd.select( dskey )
            zarr.convenience.copy( modis_sds, root, log=sys.stdout )