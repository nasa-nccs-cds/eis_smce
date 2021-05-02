import os, logging
import traitlets.config as tlc
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from dask.distributed import Client, LocalCluster
from .base import EISSingleton
import socket, threading

class DaskClusterManager(EISSingleton):


    def __init__(self, *args, **kwargs ):
        super(DaskClusterManager, self).__init__()
        self._client: Client = None
        self._cluster: LocalCluster = None

    def init_cluster( self, **kwargs ) -> Client:
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