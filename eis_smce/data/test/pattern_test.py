from intake.source.utils import reverse_format
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
import boto3

def apply_pattern( format_string: str, resolved_string: str ) -> Optional[Dict]:
    try:
        return reverse_format( format_string, resolved_string )
    except ValueError:
        return None

bucketname = 'eis-dh-fire'
s3 = boto3.resource('s3')

s3.bu



result = apply_pattern( 'data_{year}_{month}_{day}.zarr', 'data_2014_01_03.zarr' )

print( result )

