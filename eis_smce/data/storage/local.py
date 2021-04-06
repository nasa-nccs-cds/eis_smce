import traitlets.config as tlc
import fnmatch
from intake.source.utils import path_to_glob
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import glob, os

def lfm(): return LocalFileManager.instance()

class LocalFileManager(tlc.SingletonConfigurable ):

    def __init__( self, **kwargs ):
        tlc.SingletonConfigurable.__init__( self, **kwargs )

    def _parse_urlpath( self, urlpath: str ) -> str:
        return urlpath.split(":")[-1].replace("//","/").replace("//","/")

    def get_file_list(self, urlpath: str ) -> List[Dict]:
        from intake.source.utils import reverse_format
        filepath_pattern = self._parse_urlpath( urlpath )
        filepath_glob = path_to_glob( filepath_pattern )
        input_files = glob.glob(filepath_glob)
        files_list = []
        print(f" Processing {len(input_files)} input files from glob '{filepath_glob}'")
        for file_path in input_files:
            try:
                file_name, file_pattern = os.path.basename(file_path) , os.path.basename(filepath_pattern)
                print(f" reverse_format( {file_pattern}: {file_name} )")
                metadata = reverse_format( file_pattern, file_name )
                print( f" reverse_format result -> {metadata}")
                metadata['resolved'] = file_path
                files_list.append(metadata)
            except ValueError as err:
                print( f" Metadata processing error: {err}, Did you mix glob and pattern in file name?")
        return files_list