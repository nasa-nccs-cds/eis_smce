# Configuration Parameters

Each eis_smce workflow takes a run configuration file as input, which specifies 
all execution parameters for the workflow. The installation process places sample 
copies of all eis_smce run configuration files (*.cfg) in the directory *~/.eis_smce/config*.  

## Parameter Descriptions

---
### Zarr Conversion Workflow Parameters

* **input_dir**: Base directory for all input data.
* **output_dir**: Base directory for generated zarr datasets.
* **input_dset**: Glob pattern describing the input datset, relative to the *input_dir*.
* **output_dset**: Path of the output zarr dataset (without .zarr) relative to the *output_dir*
* **cache**: Path to cache directory
* **merge_dim**: Name of coordinate to concatenate over in the merge operation (default: time). If this parameter does not exist it will be created.
* **time_format**: strptime format for extracting a date/time value from a file name.  Can be used to set the value of the *merge_dim* coordinate if needed. 
* **batch_size**: Maximum number of files to be processed in a merge operation (default 1000).  Reduce this parameter if you run out of memory.
* **bucket**: Bucket name for upload.  Used only to provide the upload URL for the next stage.

#### Example

```
input_dir   = /discover/nobackup/projects/eis_freshwater
output_dir  = /gpfsm/dnb43/projects/p151/zarr
input_dset  = lahmers/RUN/1km_DOMAIN_DAens20_MCD15A2H.006_2019Flood/OUTPUT/SURFACEMODEL/**/LIS_HIST*.nc
output_dset = LIS/DA_1km/MODIS_Flood_2019/SURFACEMODEL/LIS_HIST.d01
cache       = /gpfsm/dnb43/projects/p151/zarr
merge_dim   = time
time_format = %Y%m%d%H%M
batch_size  = 1000
bucket      = eis-dh-hydro
```

---
