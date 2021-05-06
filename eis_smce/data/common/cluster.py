import logging, numpy as np
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from dask.distributed import Client, LocalCluster
from .base import EISSingleton

class DaskClusterManager(EISSingleton):

    def __init__(self, *args, **kwargs ):
        super(DaskClusterManager, self).__init__()
        self._client: Client = None
        self._cluster: LocalCluster = None

    def init_cluster( self, **kwargs ) -> Client:
        logger = logging.getLogger("distributed.utils_perf")
        logger.setLevel(logging.ERROR)
        if self._cluster is not None:
            self._cluster.close()
            self._client.close()
        self._cluster = LocalCluster( **kwargs )
        self._client = Client( self._cluster )
        return self._client

    @property
    def client(self) -> Client:
        return self._client

def dcm(): return DaskClusterManager.instance()



class ClusterInformationManager(EISSingleton):

    def __init__( self ):
        super(ClusterInformationManager, self).__init__()
        self._parameters = {}

    def set( self, pname: str, pval ):
        self._parameters[ pname ] = pval

    def add( self, pname: str, pval ):
        self._parameters.setdefault( pname, [] ).append( pval )

    def get( self, pname, default = None ):
        return self._parameters.get( pname, default )

    def ave( self, pname ):
        return np.array( self.get( pname ) ).mean()

    def test_equal( self, pname, pvalue ):
        if pname in self._parameters:
            return  ( pvalue == self._parameters[pname] )
        else:
            self.set( pname, pvalue )
            return True

def cim(): return ClusterInformationManager.instance()