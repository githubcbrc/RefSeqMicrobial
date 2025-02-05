#!/bin/bash
#SBATCH --job-name=RefSeqDownload
#SBATCH --output=download_%j.out
#SBATCH --error=download_%j.err
#SBATCH --nodes=5
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=20
#SBATCH --time=24:00:00
#SBATCH --mem=50G



srun python3 parallel_download.py 20 /download/destination/path 


