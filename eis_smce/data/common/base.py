import os, logging
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import socket, threading

class EISSingleton():
    _instance = None

    def __init__(self, *args, **kwargs ):
        self._logger = None

    @property
    def logger(self):
        self.setup_logging()
        return self._logger

    @staticmethod
    def item_path( path: str) -> str:
        return path.split(":")[-1].replace("//", "/").replace("//", "/")

    @classmethod
    def instance(cls, *args, **kwargs):
        if cls._instance is None:
            inst = cls(*args, **kwargs)
            cls._instance = inst
            cls._instantiated = cls
        return cls._instance

    @property
    def hostname(self):
        return socket.gethostname()

    @property
    def pid(self):
        return os.getpid()

    @classmethod
    def initialized(cls):
        """Has an instance been created?"""
        return hasattr(cls, "_instance") and cls._instance is not None

    def setup_logging(self):
        if self._logger is None:
            self._logger = logging.getLogger('eis_smce.intake')
            self._logger.setLevel(logging.DEBUG)
            log_file = f'{eisc().cache_dir}/logging/eis_smce.{self.hostname}.{self.pid}.log'
            print( f" ***   Opening Log file: {log_file}  *** ")
            os.makedirs( os.path.dirname(log_file), exist_ok=True )
            fh = logging.FileHandler( log_file )
            fh.setLevel(logging.DEBUG)
            ch = logging.StreamHandler()
            ch.setLevel(logging.ERROR)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            self._logger.addHandler(fh)
            self._logger.addHandler(ch)

class EISConfiguration( EISSingleton ):

    def __init__( self, **kwargs ):
        super(EISConfiguration, self).__init__(**kwargs)
        self._config = dict( cache_dir= os.path.expanduser("~/.eis_smce/cache") )

    @property
    def cache_dir(self):
        _cache_dir = self._config['cache_dir']
        os.makedirs( _cache_dir, exist_ok=True )
        return _cache_dir

    def configure(self, **kwargs):
        self._config.update( kwargs )

    def get(self, key: str, default = None ):
        return self._config.get( key, default )

    def progressBar( self, iterable, prefix='Progress:', suffix='Complete', decimals=1, length=50, fill='█', printEnd="\r"):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        items = list( iterable )
        total = len(items)
        def printProgressBar(iteration):
            percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
            filledLength = int(length * iteration // total)
            bar = fill * filledLength + '-' * (length - filledLength)
            print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
        printProgressBar(0)
        for i, item in enumerate(items):
            yield item
            printProgressBar(i + 1)
        print()

def eisc(**kwargs) -> EISConfiguration:
    _eisc = EISConfiguration.instance()
    _eisc.configure( **kwargs )
    return _eisc
