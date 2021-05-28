import os, logging
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import socket, threading

class EISSingleton():
    _instance = None

    def __init__(self, *args, **kwargs ):
        pass

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
    def logger(self):
        return EISConfiguration.get_logger()

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

class EISConfiguration( EISSingleton ):

    def __init__( self, **kwargs ):
        super(EISConfiguration, self).__init__(**kwargs)
        self._config = dict( cache_dir= os.path.expanduser("~/.eis_smce/cache"), merge_dim = "time" )

    @classmethod
    def hostname(cls):
        return socket.gethostname()

    @classmethod
    def pid(cls):
        return os.getpid()

    @property
    def cache_dir(self):
        _cache_dir = self._config['cache_dir']
        os.makedirs( _cache_dir, exist_ok=True )
        return _cache_dir

    def configure(self, **kwargs):
        self._config.update( kwargs )

    def get(self, key: str, default = None ):
        return self._config.get( key, default )

    def __getitem__(self, key: str ) -> str:
        return self._config.get( key, None )

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

    @classmethod
    def get_logger(cls):
        _logger = logging.getLogger(f'eis_smce.intake.{cls.hostname()}.{cls.pid()}')
        if len( _logger.handlers ) == 0:
            _logger.setLevel(logging.DEBUG)
            log_file = f'{eisc().cache_dir}/logging/eis_smce.{cls.hostname()}.{cls.pid()}.log'
            print( f" ***   Opening Log file: {log_file}  *** ")
            os.makedirs( os.path.dirname(log_file), exist_ok=True )
            fh = logging.FileHandler( log_file )
            fh.setLevel(logging.DEBUG)
            ch = logging.StreamHandler()
            ch.setLevel(logging.ERROR)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            _logger.addHandler(fh)
            _logger.addHandler(ch)
        return _logger

def eisc(**kwargs) -> EISConfiguration:
    _eisc = EISConfiguration.instance()
    _eisc.configure( **kwargs )
    return _eisc

def eisc_config( config_file_path: str ) -> EISConfiguration:
    _eisc = EISConfiguration.instance()
    params = {}
    with open(config_file_path) as config_file:
        for line in config_file.readlines():
            if not line.strip().startswith("#"):
                toks = line.split("=")
                if len( toks ) == 2:
                    params[ toks[0].strip() ] = toks[1].strip()
                else:
                    _eisc.logger.warn( f"Skipping line in config file: '{line}'")
    _eisc.configure( **params )
    return _eisc
