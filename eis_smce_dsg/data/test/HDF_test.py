from pyhdf.SD import SD, SDC, SDS
import rasterio


test_file = "/Users/tpmaxwel/Dropbox/HDF_file/MOD05_L2.A2021080.0000.061.NRT.hdf"

def pyhdf_read( file_path: str ):
    sd = SD( file_path, SDC.READ )
    for (akey, aval) in sd.attributes().items():
        print( f"{akey} = {aval}")


def rasterio_read( file_path: str ):
    src = rasterio.open(file_path)
    print(f"shape = {src.shape}")


rasterio_read( test_file )
