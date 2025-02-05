# RefSeqMicrobial

This repo contains a python script for downloading microbial genomes from RefSeq. The script is very compact and mostly self explanatory. Running ``python3 refseq_download.py`` will start downloading the genome data for viral, archaeal, and fungal species/strains. The bacterial data however is limited to representative/reference genomes, due to the significant size of the full dataset. 

A new script has been added to run the downloads in parallel in an HPC cluster using mpi4py. A job submission script is also included. 
