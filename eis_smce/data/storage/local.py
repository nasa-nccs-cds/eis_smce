from eis_smce.data.common.base import EISSingleton
from enum import Enum
from intake.source.utils import path_to_glob
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from functools import partial
import xarray as xa
import glob, os
from datetime import datetime

def lfm(): return LocalFileManager.instance()
def has_char(string: str, chars: str): return 1 in [c in string for c in chars]

class FileSortKey(Enum):
    filename = 1
    pattern = 2
    coordinate = 3

    def key(self, collection_specs: Dict, file_specs: Dict ):
        return self.sort_key_method( collection_specs, file_specs )

    @property
    def sort_key_method( self ):
        if self == self.filename:   return self.filename_key
        if self == self.pattern:    return self.pattern_key
        if self == self.coordinate: return self.coordinate_key
        raise Exception( f"Unknown sort_key_method: {self} vs {self.filename}" )

    @staticmethod
    def filename_key( collection_specs: Dict, file_specs: Dict ):
        return os.path.basename( file_specs['resolved'] )

    @staticmethod
    def pattern_key( collection_specs: Dict, file_specs: Dict ):
        merge_dim = collection_specs.get('merge_dim','time')
        time_format = collection_specs.get('time_format', None )
        return file_specs[merge_dim] if time_format is None else datetime.strptime( file_specs[merge_dim], time_format)

    @staticmethod
    def coordinate_key( collection_specs: Dict, file_specs: Dict ):
        with xa.open_dataset( file_specs['resolved'] ) as dset:
            merge_dim = collection_specs.get('merge_dim', 'time')
            return  dset[merge_dim].values[0]

class LocalFileManager(EISSingleton ):

    def __init__( self, **kwargs ):
        EISSingleton.__init__( self, **kwargs )

    def _parse_urlpath( self, urlpath: str ) -> str:
        return urlpath.split(":")[-1].replace("//","/").replace("//","/")

    @staticmethod
    def sort_key( item: Dict ):
        return item['sort_key']

    def get_file_list(self, urlpath: str, collection_specs: Dict ) -> List[Dict]:
        from intake.source.utils import reverse_format
        filepath_pattern = self._parse_urlpath( urlpath )
        filepath_glob = path_to_glob( filepath_pattern )
        input_files = glob.glob(filepath_glob)
        file_sort = FileSortKey[ collection_specs.get('sort', 'filename') ]
        is_glob = has_char( filepath_pattern, "*?[" )
        files_list = []
        self.logger.info(f" Processing {len(input_files)} input files from glob '{filepath_glob}'")
        for file_path in input_files:
            try:
                (file_name, file_pattern) = (os.path.basename(file_path) , os.path.basename(filepath_pattern)) if is_glob else (file_path,filepath_pattern)
                metadata = reverse_format( file_pattern, file_name )
                metadata['resolved'] = file_path
                metadata['sort_key'] = file_sort.key( collection_specs, metadata )
                files_list.append(metadata)
            except ValueError as err:
                self.logger.error( f" Metadata processing error: {err}, Did you mix glob and pattern in file name?")
        files_list.sort( key=self.sort_key )
        for fi in range( 0, len(files_list), 100 ):
            self.logger.info( files_list[ fi ]['resolved'] )
        return files_list
