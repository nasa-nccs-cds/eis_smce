import rioxarray as rxr


cache_file = "/home/jovyan/.eis_smce/cache/MOD14.A2020298.1835.061.2020348153757.hdf"

modis_pre = rxr.open_rasterio(cache_file)

print( type(modis_pre) )