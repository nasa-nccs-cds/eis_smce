import traitlets.config as tlc
import fnmatch
from intake.source.utils import path_to_glob
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import glob

def lfm(): return LocalFileManager.instance()

class LocalFileManager(tlc.SingletonConfigurable ):

    def __init__( self, **kwargs ):
        tlc.SingletonConfigurable.__init__( self, **kwargs )

    def _parse_urlpath( self, urlpath: str ) -> str:
        return urlpath.split(":")[-1].replace("//","/").replace("//","/")

    def get_file_list(self, urlpath: str ) -> List[Dict]:
        from intake.source.utils import reverse_format
        def has_char(string: str, chars: str): return 1 in [c in string for c in chars]
        filepath_pattern = self._parse_urlpath( urlpath )
        is_glob = has_char( filepath_pattern, "*?[" )
        filepath_glob = path_to_glob( filepath_pattern )
        files_list = []
        for file_path in  glob.glob(filepath_glob):
            try:
                metadata = {} if is_glob else reverse_format( filepath_pattern, file_path )
                metadata['resolved'] = file_path
                files_list.append(metadata)
            except ValueError:
                pass
        return files_list