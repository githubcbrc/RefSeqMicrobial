import os
import pandas as pd
import urllib.request
import numpy as np
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def download_assembly_summary(url, output_filename):
    """Download assembly summary file if it doesn't exist."""
    if os.path.exists(output_filename):
        print(f"Skipping {output_filename}, already exists.")
        return output_filename
    try:
        print(f"Node {rank} downloading {output_filename} ...")
        urllib.request.urlretrieve(url, output_filename)
        print(f"Downloaded {output_filename}")
        return output_filename
    except Exception as e:
        print(f"Error downloading {output_filename}: {e}")
        return None

def download_genome(ftp_url, output_dir):
    """Download genome file if it doesn't already exist."""
    filename = os.path.basename(ftp_url) + "_genomic.fna.gz"
    output_path = os.path.join(output_dir, filename)

    if os.path.exists(output_path):
        print(f"Skipping {filename}, already exists.")
        return

    try:
        urllib.request.urlretrieve(ftp_url, output_path)
        print(f"Downloaded {filename}")
    except Exception as e:
        print(f"Error downloading {filename}: {e}")

def download_in_parallel(df, output_dir, num_threads):
    """Download genomes in parallel using multiple threads."""
    from multiprocessing.dummy import Pool as ThreadPool
    os.makedirs(output_dir, exist_ok=True)
    
    pool = ThreadPool(num_threads)
    ftp_urls = df["ftp_path"].apply(lambda x: x + "/" + os.path.basename(x) + "_genomic.fna.gz")
    pool.starmap(download_genome, [(url, output_dir) for url in ftp_urls])
    pool.close()
    pool.join()

def main(num_threads):
    urls = {
        "archaea": "https://ftp.ncbi.nlm.nih.gov/genomes/refseq/archaea/assembly_summary.txt",
        "bacteria": "https://ftp.ncbi.nlm.nih.gov/genomes/refseq/bacteria/assembly_summary.txt",
        "viral": "https://ftp.ncbi.nlm.nih.gov/genomes/refseq/viral/assembly_summary.txt",
        "fungi": "https://ftp.ncbi.nlm.nih.gov/genomes/refseq/fungi/assembly_summary.txt",
        "human": "https://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/assembly_summary.txt"
    }

    data_path = "/ibex/user/salhia/RefSeq"
    os.makedirs(data_path, exist_ok=True)
    os.chdir(data_path)

    if rank == 0:
        for domain, url in urls.items():
            assembly_summary_file = f"assembly_summary_{domain}.txt"
            urllib.request.urlretrieve(url, assembly_summary_file)
    
    comm.Barrier()  # Synchronize nodes before proceeding

    # Each node processes a subset of each domain's data
    for domain in urls.keys():
        assembly_summary_file = f"assembly_summary_{domain}.txt"
        
        if not os.path.exists(assembly_summary_file):
            print(f"Skipping {domain}, assembly summary not found.")
            continue

        df = pd.read_csv(assembly_summary_file, sep="\t")
        df_split = np.array_split(df, size)[rank]
        download_in_parallel(df_split, f"{domain}_genomes", num_threads)

if __name__ == "__main__":
    import sys
    num_threads = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    main(num_threads)

