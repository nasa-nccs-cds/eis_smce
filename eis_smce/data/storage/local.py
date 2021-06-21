from eis_smce.data.common.base import EISSingleton, eisc
from enum import Enum
from intake.source.utils import path_to_glob
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable, Set, Iterable
import numpy as np
from eis_smce.data.common.cluster import dcm, cim
import dask, xarray as xa
from functools import partial
import glob, os, time
from datetime import datetime
from eis_smce.data.common.base import eisc, EISConfiguration, EISSingleton as eiss

def has_char(string: str, chars: str): return 1 in [c in string for c in chars]
def skey( svals: Iterable[str] ):
    sv = list(svals)
    sv.sort()
    return "-".join(sv)

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
        merge_dim = eisc().get('merge_dim')
        time_format = eisc().get('time_format', None )
        return file_specs[merge_dim] if time_format is None else datetime.strptime( file_specs[merge_dim], time_format)

    @staticmethod
    def coordinate_key( file_specs: Dict ):
        with xa.open_dataset( file_specs['resolved'] ) as dset:
            merge_dim = eisc().get('merge_dim')
            return  dset[merge_dim].values[0]

class DatasetSegmentSpec:

    def __init__(self, name: str,  dims: List[int],  vlist: Set[str] ):
        self._name = name
        self._dims = dims
        self._file_specs: List[ Dict[str, str] ] = []
        self._vlist: Set[str] = vlist

    @property
    def name(self):
        return self._name

    @property
    def dims(self):
        return self._dims

    def get_vlist(self) -> Set[str]:
        return self._vlist

    def is_empty(self):
        return len( self._file_specs ) == 0

    def size(self):
        return len( self._file_specs )

    def get_file_specs(self) -> List[Dict[str, str]]:
        return self._file_specs

    def add_file_spec(self, fspec: Dict[str,str]):
        self._file_specs.append( fspec )

    def add_file_specs(self, fspecs: Iterable[Dict[str,str]]):
        self._file_specs.extend( fspecs )

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

    def equal_attr(self, v0: Union[str,np.array], v1: Union[str,np.array] ) -> bool:
        if v1 is None: return False
        if type(v0) is np.ndarray:    return np.array_equal( v0, v1 )
        if type(v0) is xa.DataArray:  return v0.equals( v1 )
        return v0 == v1

    def get_vlists(self) -> List[List[List[int],Set[str]]]:
        return [ [ ss.dims, ss.get_vlist() ] for ss in self._segment_specs.values() ]

    def get_file_specs(self, vlist: Set[str] ) -> List[Dict[str,str]]:
        return self._segment_specs[ skey(vlist) ].get_file_specs()

    def get_segment_size(self, vlist: Set[str] ) -> int:
        return self._segment_specs[ skey(vlist) ].size()

    def get_segment_name(self, vlist: Set[str] ) -> int:
        return self._segment_specs[ skey(vlist) ].name

    def get_segment_spec(self, vlist: Set[str] ) -> DatasetSegmentSpec:
        return self._segment_specs[ skey(vlist) ]

    def get_dynamic_attributes(self) -> Set[str]:
        return self._dynamic_attributes

    def get_file_key(self, file_path: str ) -> str:
        with xa.open_dataset( file_path ) as dset:
            data_vars: List[str] = [ str(v) for v in dset.data_vars.keys() ]
            data_vars.sort()
            return "-".join( data_vars )

    @staticmethod
    def _get_file_metadata( merge_dim: str,  file_path: str ) -> Tuple[ Dict[str,Union[str,np.array]], Optional[Set[str]] ]:
        try:
            with xa.open_dataset(file_path) as dset:
                _attrs: Dict[str,Union[str,np.array]] = { str(k): v for k, v in dset.attrs.items() }
                _attrs['dims'] = dset.dims
                _vset: Set[str] = { str(v) for v in dset.data_vars.keys() }
                return ( _attrs,  _vset )
        except Exception as err:
            return ( dict( exc=err, estr=str(err), file=file_path ), None )

    def _process_files_metadata(self, fdata: List[ Tuple[ Dict[str,Union[str,np.array]], Set[str] ] ] ):
        for ( _attrs, _vlist ) in fdata:
            dims = _attrs['dims']
            if _vlist is None:
                print( f"\n ***** Error reading file {_attrs['file']}: {_attrs['estr']}\n")
                raise( _attrs['exc'] )
            if self._base_metadata is None:
                self._base_metadata =  _attrs
            else:
                for k,v in _attrs.items():
                    if not self.equal_attr( v, self._base_metadata.get( k, None ) ):
                        self._dynamic_attributes.add( k )
            self._file_var_sets.append( [dims. _vlist] )
        print( f"  ****  Computed Dynamic Attributes: {list(self._dynamic_attributes)}")

    @staticmethod
    def sort_key( item: Dict ):
        return item['sort_key']

    def _generate_file_specs(self, urlpath: str, **kwargs):
        from intake.source.utils import reverse_format
        filepath_pattern = eiss.item_path( urlpath )
        filepath_glob = path_to_glob( filepath_pattern )
        self._input_files = glob.glob(filepath_glob)
        assert len(self._input_files) > 0, f"Input pattern does not match any files: '{filepath_glob}'"
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

    def process_files(self, urlpath: str, **kwargs ) -> int:
        t0 = time.time()
        self._generate_file_specs( urlpath, **kwargs )
        merge_dim = eisc().get('merge_dim')
        print( "Testing varlists in all files")
        t1 = time.time()
        tasks = dcm().client.map( partial( self._get_file_metadata, merge_dim ), self._input_files )
        files_metadata = dcm().client.compute( tasks, sync=True )
        self._process_files_metadata( files_metadata )
        t2 = time.time()

        var_set_intersect: Set[str]  = self._file_var_sets[0][1].intersection( *self._file_var_sets )
        var_set_diffs: List[Set] = [ s.difference(var_set_intersect) for [d,s] in self._file_var_sets ]
        var_set_difference: Set[str] = var_set_diffs[0].union( *var_set_diffs )
        if len( var_set_intersect ) > 0: self.addSegmentSpec( "", self._file_specs.values(), var_set_intersect )
        t3 = time.time()

        print( f"Pre-Processing {len(self._input_files)} files:")
        for (f, [dims, var_set] ) in zip( self._input_files, self._file_var_sets ):
            outlier_vars: Set[str] = var_set_difference.intersection( var_set )
            if len( outlier_vars ) > 0:
                outlier_key = "_" + "-".join( outlier_vars )
                self.addSegmentSpec( outlier_key, [ self._file_specs[f] ], dims, outlier_vars )

        for segment_spec in self._segment_specs.values(): segment_spec.sort( key=self.sort_key )
        t4 = time.time()
        print(f"Done preprocessing with times {t1-t0:.2f} {t2-t1:.2f} {t3-t2:.2f} {t4-t3:.2f}")

    def addSegmentSpec(self, name: str, file_specs: Iterable[Dict[str, str]], dims: Set[str], vlist: Set[str] ):
        seg_spec: DatasetSegmentSpec = self._segment_specs.setdefault( skey(vlist), DatasetSegmentSpec( name, dims, vlist ))
        if seg_spec.is_empty(): print(f"Adding segment: name='{name}', vars = {list(vlist)}, dims = {list(dims)}")
        seg_spec.add_file_specs( file_specs )
        return seg_spec
