import io, os, sys
from eis_smce.data.common.base import EISSingleton, eisc
from pyhdf.SD import SD, SDC, SDS
from collections.abc import MutableMapping
from botocore.client import BaseClient
import zarr

class zarrModisDS():

    def __init__( self, client: BaseClient, store: MutableMapping, **kwargs  ):
        self._store: MutableMapping = store
        self._client: BaseClient = client

    def from_s3(self, bucketname: str, itemname: str ):
        file_name = itemname.split("/")[-1]
        modis_filepath = os.path.join( eisc().cache_dir, file_name )
        self._client.download_file(bucketname, itemname, modis_filepath)
        modis_sd: SD = SD(modis_filepath, SDC.READ)
        root = zarr.group(store=self._store)

        for (akey, aval) in modis_sd.attributes().items():
            if akey.startswith("CoreMetadata"):
                pass
            else:
                root.attrs[akey] = aval

        for dskey, dsinfo in modis_sd.datasets().items():
            modis_sds: SDS = modis_sd.select( dskey )
            zarr.convenience.copy( modis_sds, root, log=sys.stdout )