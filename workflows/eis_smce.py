import importlib
import os
import sys
import pathlib

# pathlib.Path(__file__).parent.absolute()
sys.path.insert(0, os.path.abspath('../source'))
__file__ = {'sys': sys, 'importlib': importlib}

del importlib
del os
del sys

__file__['importlib'].reload(__file__['sys'].modules[__name__])
