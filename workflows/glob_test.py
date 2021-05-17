import glob
gpath='/discover/nobackup/projects/eis_freshwater/lahmers/RUN/1km_DOMAIN_DAens20_MCD15A2H.006_2019Flood/OUTPUT/SURFACEMODEL/**/LIS_HIST*.nc'
file_list = glob.glob( gpath )
print( f" Found {len(file_list)} files.")

