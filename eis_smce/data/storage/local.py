from eis_smce.data.common.base import EISSingleton, eisc
from enum import Enum
from intake.source.utils import path_to_glob
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable, Set
import numpy as np
import xarray as xa
import glob, os, time
from datetime import datetime

def has_char(string: str, chars: str): return 1 in [c in string for c in chars]

class FileSortKey(Enum):
    filename = 1
    pattern = 2
    coordinate = 3

    def key(self, file_specs: Dict ):
        return self.sort_key_method( file_specs )

    @property
    def sort_key_method( self ):
        if self == self.filename:   return self.filename_key
        if self == self.pattern:    return self.pattern_key
        if self == self.coordinate: return self.coordinate_key
        raise Exception( f"Unknown sort_key_method: {self} vs {self.filename}" )

    @staticmethod
    def filename_key( file_specs: Dict ):
        return os.path.basename( file_specs['resolved'] )

    @staticmethod
    def pattern_key( file_specs: Dict ):
        merge_dim = eisc().get('merge_dim','time')
        time_format = eisc().get('time_format', None )
        return file_specs[merge_dim] if time_format is None else datetime.strptime( file_specs[merge_dim], time_format)

    @staticmethod
    def coordinate_key( file_specs: Dict ):
        with xa.open_dataset( file_specs['resolved'] ) as dset:
            merge_dim = eisc().get('merge_dim', 'time')
            return  dset[merge_dim].values[0]

class DatasetSegmentSpec:

    def __init__(self, name: str, file_specs: List[Dict[str, str]], vlist: Set[str] ):
        self._name = name
        self._file_specs: List[Dict[str, str]] = file_specs
        self._vlist: Set[str] = vlist

    def get_file_specs(self) -> List[Dict[str, str]]:
        return self._file_specs

    def add_file_spec(self, fspec: Dict[str,str]):
        self._file_specs.append( fspec )

    def sort( self, key ):
        self._file_specs.sort( key=key )

class SegmentedDatasetManager:

    def __init__(self):
        self._segment_specs: Dict[ Set[str], DatasetSegmentSpec ] = {}
        self._dynamic_attributes = set()
        self._base_metadata: Dict = None
        self._file_var_sets = []
        self._input_files = None
        self._file_specs: Dict[str,Dict[str,str]] = {}

    def equal_attr(self, v0, v1 ) -> bool:
        if v1 is None: return False
        if type(v0) is np.ndarray:    return np.array_equal( v0, v1 )
        if type(v0) is xa.DataArray:  return v0.equals( v1 )
        return v0 == v1

    def get_vlists(self) -> List[Set[str]]:
        return self._file_var_sets

    def get_file_specs(self, vlist: Set[str] ) -> List[Dict[str,str]]:
        return self._segment_specs[vlist].get_file_specs()

    def get_segment_spec(self, vlist: Set[str] ) -> DatasetSegmentSpec:
        return self._segment_specs[ vlist ]

    def get_file_key(self, file_path: str ) -> str:
        with xa.open_dataset( file_path ) as dset:
            data_vars: List[str] = [ str(v) for v in dset.data_vars.keys() ]
            data_vars.sort()
            return "-".join( data_vars )

    def _process_file(self, file_path: str ):
        with xa.open_dataset(file_path) as dset:
            if self._base_metadata is None:
                self._base_metadata =  dset.attrs.copy()
            else:
                for k,v in dset.attrs.items():
                    if not self.equal_attr( dset.attrs[k], self._base_metadata.get( k, None ) ):
                        self._dynamic_attributes.add( str(k) )
            self._file_var_sets.append( { str(v) for v in dset.data_vars.keys() } )

    def _parse_urlpath( self, urlpath: str ) -> str:
        return urlpath.split(":")[-1].replace("//","/").replace("//","/")

    @staticmethod
    def sort_key( item: Dict ):
        return item['sort_key']

    def _generate_file_specs(self, urlpath: str, **kwargs):
        from intake.source.utils import reverse_format
        filepath_pattern = self._parse_urlpath( urlpath )
        filepath_glob = path_to_glob( filepath_pattern )
        self._input_files = glob.glob(filepath_glob)
        file_sort = FileSortKey[ kwargs.get('sort', 'filename') ]
        is_glob = has_char( filepath_pattern, "*?[" )
        eisc().logger.info(f" Processing {len(self._input_files)} input files from glob '{filepath_glob}'")

        for file_path in self._input_files:
            try:
                (file_name, file_pattern) = (os.path.basename(file_path) , os.path.basename(filepath_pattern)) if is_glob else (file_path,filepath_pattern)
                metadata: Dict[str,str] = reverse_format( file_pattern, file_name )
                metadata['resolved'] = file_path
                metadata['sort_key'] = file_sort.key( metadata )
                self._file_specs[ file_path ] = metadata
            except ValueError as err:
                eisc().logger.error( f" Metadata processing error: {err}, Did you mix glob and pattern in file name?")

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

        # Progress Bar Printing Function
        def printProgressBar(iteration):
            percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
            filledLength = int(length * iteration // total)
            bar = fill * filledLength + '-' * (length - filledLength)
            print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)

        # Initial Call
        printProgressBar(0)
        # Update Progress Bar
        for i, item in enumerate(items):
            yield item
            printProgressBar(i + 1)
        # Print New Line on Complete
        print()

    def process_files(self, urlpath: str ):
        t0 = time.time()
        self._generate_file_specs( urlpath )

        print( "Testing varlists in all files")
        t1 = time.time()
        for f in self._input_files:
            self._process_file( f )
        t2 = time.time()

        var_set_intersect: Set[str]  = self._file_var_sets[0].intersection( *self._file_var_sets )
        var_set_difference: Set[str] = set([]).union( [ s.difference(var_set_intersect) for s in self._file_var_sets ] )
        if len( var_set_intersect ) > 0: self._segment_specs[ var_set_intersect ] = DatasetSegmentSpec("", list(self._file_specs.values()), var_set_intersect)
        t3 = time.time()

        print( f"Pre-Processing {len(self._input_files)} files:")
        for (f, var_set) in self.progressBar( zip( self._input_files, self._file_var_sets ) ):
            outlier_vars: Set[str] = var_set_difference.intersection( var_set )
            if len( outlier_vars ) > 0:
                outlier_key = "_" + "-".join( outlier_vars )
                outlier_data: DatasetSegmentSpec = self._segment_specs.setdefault( outlier_vars, DatasetSegmentSpec( outlier_key, [], outlier_vars ) )
                outlier_data.add_file_spec( self._file_specs[f] )

        for segment_spec in self._segment_specs.values(): segment_spec.sort( key=self.sort_key )
        t5 = time.time()
