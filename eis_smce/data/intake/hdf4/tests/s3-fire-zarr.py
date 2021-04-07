import os, xarray as xr
from eis_smce.data.intake.hdf4.drivers import HDF4Source
from typing import List, Union, Dict, Callable, Tuple, Optional, Any, Type, Mapping, Hashable
from intake_xarray.xzarr import ZarrSource

part_index: int = 0
data_url: str      = "s3://eis-dh-fire/mod14/raw/MOD14.{sample}.061.{tid}.hdf"  # Matches files like glob: "s3://eis-dh-fire/mod14/raw/MOD14.*.hdf"
data_file_url: str = "s3://eis-dh-fire/mod14/raw/MOD14.A2020303.0515.061.2020349105630.hdf"

if __name__ == '__main__':
    h4s: HDF4Source = HDF4Source( data_url  )                             # Creates source encapsulating all matched files in data_url
    ds0: xr.Dataset = h4s.read_partition( part_index )                    # Each partition corresponds to a single file, downloads file from s3 to local cache before reading.

    remote_input_file: str = ds0.attrs['remote_file']
    local_input_file: str  = ds0.attrs['local_file']
    remote_zarr_file: str = os.path.expanduser( "~/.eis_smce/cache/mod14/raw/MOD14.061.hdf" )

    print(f"\n HDF4Source DATASET DS0:" )
    print( f"\n ***  attributes:")
    for vid, v in ds0.attrs.items():
        print(f" ----> {vid}: {v}")
    print( f"\n ***  variables:")
    for vid, v in ds0.variables.items():
        print(f" ----> {vid}{v.dims} ({v.shape})")

    xzSources: List[ZarrSource] = h4s.export( remote_zarr_file, concat_dim='number_of_active_fires' )   # Exports the current partition (index = 0), Zarr is the default export format

    print( f"Exported file '{remote_input_file}' (cached at '{local_input_file}') to '{remote_zarr_file}'")
    print( "Catalog entry:" )

    print( xzSources[0].yaml() )



