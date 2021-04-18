import os, logging
import traitlets.config as tlc
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from dask.distributed import Client, LocalCluster
from .base import EISSingleton
import socket, threading

class DaskClusterManager(EISSingleton):


    def __init__(self, *args, **kwargs ):
        super(DaskClusterManager, self).__init__()
        self._client = None
        self._cluster = None

    def init_cluster( self, **kwargs ) -> Client:
        if self._cluster is None or kwargs.get('refresh',False):
            self._cluster = LocalCluster( **kwargs )
            self._client = Client( self._cluster )
        return self._client

    @property
    def client(self) -> Client:
        return self._client

def dcm(): return DaskClusterManager.instance()