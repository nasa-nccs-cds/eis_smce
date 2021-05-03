from eis_smce.data.common.base import EISSingleton
from enum import Enum
from intake.source.utils import path_to_glob
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable, Set
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

    def get_file_list_segments(self, files: List[str]):
        list_segments = {}
        file_var_sets = [ self.get_file_var_set(f) for f in files ]
        var_set_intersect  = file_var_sets[0].intersection( *file_var_sets )
        var_set_difference = file_var_sets[0].symmetric_difference( *file_var_sets )
        if len( var_set_intersect ) > 0: list_segments[var_set_intersect] = files
        for var_set in file_var_sets:


    def get_file_lists(self, urlpath: str, collection_specs: Dict) -> Dict[str, List[Dict]]:
        from intake.source.utils import reverse_format
        filepath_pattern = self._parse_urlpath( urlpath )
        filepath_glob = path_to_glob( filepath_pattern )
        input_files = glob.glob(filepath_glob)
        file_sort = FileSortKey[ collection_specs.get('sort', 'filename') ]
        is_glob = has_char( filepath_pattern, "*?[" )
        file_list_map: Dict[str,List[Dict]] = {}
        self.logger.info(f" Processing {len(input_files)} input files from glob '{filepath_glob}'")
        for file_path in input_files:
            try:
                file_key = self.get_file_key( file_path )
                files_list = file_list_map.setdefault( file_key, [] )
                (file_name, file_pattern) = (os.path.basename(file_path) , os.path.basename(filepath_pattern)) if is_glob else (file_path,filepath_pattern)
                metadata: Dict[str,str] = reverse_format( file_pattern, file_name )
                metadata['resolved'] = file_path
                metadata['sort_key'] = file_sort.key( collection_specs, metadata )
                files_list.append(metadata)
            except ValueError as err:
                self.logger.error( f" Metadata processing error: {err}, Did you mix glob and pattern in file name?")
        for files_list in file_list_map.values(): files_list.sort( key=self.sort_key )
        return file_list_map

    def get_file_key(self, file_path: str ) -> str:
        with xa.open_dataset( file_path ) as dset:
            data_vars: List[str] = [ str(v) for v in dset.data_vars.keys() ]
            data_vars.sort()
            return "-".join( data_vars )

    def get_file_var_set(self, file_path: str) -> Set[str]:
        with xa.open_dataset(file_path) as dset:
            return { str(v) for v in dset.data_vars.keys() }
