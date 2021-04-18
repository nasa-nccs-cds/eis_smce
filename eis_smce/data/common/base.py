import os, logging
import traitlets.config as tlc
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from traitlets.config.loader import load_pyconfig_files
from traitlets.config.loader import Config
import socket, threading

class EISSingleton(tlc.Configurable):
    _instance = None
    _config_instances: List["EISSingleton"] = []

    def __init__(self, *args, **kwargs ):
        super(EISSingleton, self).__init__()
        self.update_config( eisc().config )
        self._config_instances.append( self )
        self.logger = eisc().logger

    @classmethod
    def _walk_mro(cls):
        """Walk the cls.mro() for parent classes that are also singletons

        For use in instance()
        """

        for subclass in cls.mro():
            if issubclass(cls, subclass) and \
                    issubclass(subclass, EISSingleton) and \
                    subclass != EISSingleton:
                yield subclass

    @classmethod
    def clear_instance(cls):
        """unset _instance for this class and singleton parents.
        """
        if not cls.initialized():
            return
        for subclass in cls._walk_mro():
            if isinstance(subclass._instance, cls):
                # only clear instances that are instances
                # of the calling class
                subclass._instance = None

    @classmethod
    def instance(cls, *args, **kwargs):
        if cls._instance is None:
            inst = cls(*args, **kwargs)
            cls._instance = inst
            cls._instantiated = cls
        return cls._instance

    @classmethod
    def initialized(cls):
        """Has an instance been created?"""
        return hasattr(cls, "_instance") and cls._instance is not None

class EISConfiguration( tlc.Configurable ):
    _instance = None
    default_cache_dir = tlc.Unicode(os.path.expanduser("~/.eis_smce/cache")).tag(config=True)
    logger = None

    def __init__( self, **kwargs ):
        self.cache_dir = kwargs.pop('cache', self.default_cache_dir)
        os.makedirs( self.cache_dir, exist_ok=True )
        self.name = kwargs.pop( "name", "eis.smce" )
        self.mode =  kwargs.pop( "mode", "default" )
        self._config: Config = None
        self._configure_()
        super(EISConfiguration, self).__init__( **kwargs )
        self.setup_logging()

    @classmethod
    def instance(cls,  **kwargs):
        if cls._instance is None:
            inst = cls(**kwargs )
            cls._instance = inst
            cls._instantiated = cls
        return cls._instance

    @property
    def hostname(self):
        return socket.gethostname()

    @property
    def pid(self):
        return os.getpid()

    def getCurrentConfig(self):
        config_dict = {}
        for cfg_file in self._config_files:
            scope = self.config_scope
            config_dict[scope] = load_pyconfig_files([cfg_file], self.config_dir)
        return config_dict

    @property
    def config_scope(self):
        return f"{self.name}.{self.mode}"

    def generate_config_file(self) -> Dict:
        #        print( f"Generate config file, classes = {[inst.__class__ for inst in cls._config_instances]}")
        trait_map = self.getCurrentConfig()
        for inst in EISSingleton._config_instances:
            self.add_trait_values(trait_map, self.config_scope, inst)
        return trait_map

    @classmethod
    def add_trait_values(cls, trait_map: Dict, scope: str, instance: "EISSingleton"):
        class_traits = instance.class_traits(config=True)
        for tid, trait in class_traits.items():
            tval = getattr(instance, tid)
            trait_scope = scope
            trait_instance_values = trait_map.setdefault(trait_scope, {})
            trait_values = trait_instance_values.setdefault(instance.__class__.__name__, {})
            trait_values[tid] = tval

    def _configure_(self):
        if self._config is None:
            self._lock = threading.Lock()
            cfg_file = self.config_file( self.name, self.mode )
            (self.config_dir, fname) = os.path.split(cfg_file)
            self._config_files = [fname]
            print(f"Loading config files: {self._config_files} from dir {self.config_dir}")
            self._config = load_pyconfig_files(self._config_files, self.config_dir)
            self.update_config(self._config)

    @classmethod
    def config_file(cls, name: str, mode: str) -> str:
        config_dir = os.path.join(os.path.expanduser("~"), ".eis_smce", "config", mode)
        if not os.path.isdir(config_dir): os.makedirs(config_dir, mode=0o777)
        cfg_file = os.path.join(config_dir, name + ".py")
        if not os.path.isfile(cfg_file):
            with open(cfg_file, 'w') as fp: pass
        return cfg_file

    def save_config( self ):

        conf_dict = self.generate_config_file()
        for scope, trait_classes in conf_dict.items():
            cfg_file = os.path.realpath( self.config_file( scope, self.mode ) )
            os.makedirs(os.path.dirname(cfg_file), 0o777, exist_ok=True)
            lines = []

            for class_name, trait_map in trait_classes.items():
                for trait_name, trait_value in trait_map.items():
                    tval_str = f'"{trait_value}"' if isinstance(trait_value, str) else f"{trait_value}"
                    cfg_str = f"c.{class_name}.{trait_name} = {tval_str}\n"
                    lines.append( cfg_str )

            self.logger.info(f"Writing config file: {cfg_file}")
            with self._lock:
                cfile_handle = open(cfg_file, "w")
                cfile_handle.writelines(lines)
                cfile_handle.close()
            self.logger.info(f"Config file written")

    def setup_logging(self):
        if self.logger is None:
            self.logger = logging.getLogger('eis_smce.intake')
            self.logger.setLevel(logging.DEBUG)
            log_file = f'{self.cache_dir}/logging/eis_smce.{self.hostname}.{self.pid}.log'
            print( f"\n   ***   Opening Log file: {log_file}  ***  \n ")
            os.makedirs( os.path.dirname(log_file), exist_ok=True )
            fh = logging.FileHandler( log_file )
            fh.setLevel(logging.DEBUG)
            ch = logging.StreamHandler()
            ch.setLevel(logging.ERROR)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

def eisc(**kwargs) -> EISConfiguration:
    return EISConfiguration.instance(**kwargs)
