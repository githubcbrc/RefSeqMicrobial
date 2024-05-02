#!/usr/bin/env python3
import os
import pandas as pd
import urllib.request

def download_assembly_summary(url, output_filename):
    """Download assembly summary file from the given URL."""
    try:
        filename, _ = urllib.request.urlretrieve(url, output_filename)
        return filename
    except Exception as e:
        print(f"Error downloading assembly summary from {url}: {e}")
        return None

def preprocess_assembly_summary(filename):
    """Preprocess the assembly summary file by removing comments and establishing the header."""
    try:
        with open(filename, 'r') as file:
            lines = file.readlines()
            if len(lines) > 1:
                del lines[0]  # Remove the first line
                lines[0] = lines[0].replace('#', '')

        with open(filename, 'w') as updated_file:
            updated_file.writelines(lines)
    except Exception as e:
        print(f"Error preprocessing assembly summary file: {e}")

def read_assembly_summary(filename):
    """Read the assembly summary file into a pandas DataFrame."""

    # Define a custom function to handle non-integer values
    def custom_converter(value):
        try:return int(value)
        except ValueError:return None  # or any other appropriate value

    try:
        converter_options = 	{
				'total_gene_count':custom_converter, 
				'protein_coding_gene_count':custom_converter, 
				'non_coding_gene_count':custom_converter
				}
        df = pd.read_csv(filename, sep='\t', converters=converter_options )
        return df
    except Exception as e:
        print(f"Error reading assembly summary file: {e}")
        return None

def filter_bacterial_genomes(df):
    """Filter bacterial genomes DataFrame to include only representative and reference genomes."""
    try:
        return df[(df["refseq_category"] == "representative genome") | (df["refseq_category"] == "reference genome")]
    except Exception as e:
        print(f"Error filtering bacterial genomes: {e}")
        return None

def download_genomes(df, output_dir):
    """Download genomes from the DataFrame and save to the output directory."""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for _, row in df.iterrows():
            ftp_url = row["ftp_path"]
            filename = os.path.basename(ftp_url)
            filename += "_genomic.fna.gz"
            ftp_url += "/" + filename
            output_path = os.path.join(output_dir, filename)
            urllib.request.urlretrieve(ftp_url, output_path)
            print(f"Downloaded genome: {filename}")
    except Exception as e:
        print(f"Error downloading genomes: {e}")

# URLs for assembly summary files
urls = {
    "archaea": "https://ftp.ncbi.nlm.nih.gov/genomes/refseq/archaea/assembly_summary.txt",
    "bacteria": "https://ftp.ncbi.nlm.nih.gov/genomes/refseq/bacteria/assembly_summary.txt",
    "viral": "https://ftp.ncbi.nlm.nih.gov/genomes/refseq/viral/assembly_summary.txt",
    "fungi": "https://ftp.ncbi.nlm.nih.gov/genomes/refseq/fungi/assembly_summary.txt"
}

# Download assembly summaries and genomes for each domain
for domain, url in urls.items():
    print(f"Downloading assembly summary for {domain}...")
    assembly_summary_file = download_assembly_summary(url, f"assembly_summary_{domain}.txt")

    if assembly_summary_file:
        preprocess_assembly_summary(assembly_summary_file)
        df = read_assembly_summary(assembly_summary_file)

        if df is not None:
            if domain == "bacteria":
                df = filter_bacterial_genomes(df)

            if df is not None:
                download_genomes(df, f"{domain}_genomes")

