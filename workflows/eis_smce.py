import importlib
import os
import sys
from pathlib import Path

source_root =  f"{Path().absolute().parent}/source"
sys.path.insert( 0, source_root )
__file__ = { 'sys': sys, 'importlib': importlib }

del importlib
del os
del sys

__file__['importlib'].reload(__file__['sys'].modules[__name__])
