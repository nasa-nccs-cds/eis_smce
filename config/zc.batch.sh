#!/bin/bash
#SBATCH --job-name=parallel_job      # Job name
#SBATCH --nodes=1                    # Run all processes on a single node
#SBATCH --ntasks=1                   # Run a single task
#SBATCH --cpus-per-task=4            # Number of CPU cores per task
#SBATCH --mem=4gb                    # Job memory request
#SBATCH --time=06:00:00              # Time limit hrs:min:sec
#SBATCH --output=eis_smce.stdout.log # Standard output and error log
pwd; hostname; date

echo "Running zarr conversion on $SLURM_CPUS_ON_NODE CPU cores"

cd /discover/nobackup/${USER}/eis_smce
source /home/${USER}/.bashrc
source activate eis_smce
python ./workflows/zarr_conversion.py /home/${USER}/.eis_smce/config/chunking_tests/zc2.cfg

date
