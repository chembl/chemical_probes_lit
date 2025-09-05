###  Author: Melissa F. Adasme ###

"""
Subsetting to accepted probe's targets
By the definition of chemical probes, they are meant to act towards specific targets or family of targets. 
The NER dataset contains all sentences matching a chemical probe but without checking if that targets is the correct one. 
This notebook uses the HQ probes dataset from ChEMBL to filter that.
"""


import pandas as pd
import requests
import argparse

def main(input_path, probes_path):
    ## 1. Loading ner data with triplets
    ner_data = pd.read_csv(input_path, sep='\t')
    print("✅ Loaded NER probes in triples from step 2.", flush=True)
    
    ## 2. Loading main HQ probes dataset and extracting list of genes
    hqprobes = pd.read_csv(probes_path, sep="\t")
    print("✅ Loaded HQ probe labels.", flush=True)
    
    ## 3. mapping genes to ENSEMBL ids
    def map_gene_ids_to_ensembl(gene_ids, species='human'):
        """
        Maps a list of gene IDs to Ensembl IDs using the Ensembl REST API.
        Handles comma-separated gene names by exploding them before mapping.
        Args:
          gene_ids: A list of gene IDs (may include comma-separated names).
          species: The species to use for the mapping. Default is 'human'.
        Returns:
          A dictionary mapping gene IDs to Ensembl IDs.
        """
        ensembl_mapping = {} #dictionary for mapping
        for gene_id in gene_ids:
          # Split comma-separated gene names
          for gene_name in gene_id.split(','):
            gene_name = gene_name.strip()  # Remove any leading/trailing whitespace
            url = f'https://rest.ensembl.org/lookup/symbol/{species}/{gene_name}?expand=1'
            headers = {'Content-Type': 'application/json'}
            response = requests.get(url, headers=headers)
            if response.ok:
              data = response.json()
              if 'id' in data:
                ensembl_mapping[gene_name] = data['id']
              else:
                ensembl_mapping[gene_name] = None
                print(gene_name)
            else:
              print(f"Error retrieving data for {gene_name}: {response.status_code}")
              ensembl_mapping[gene_name] = None
        return ensembl_mapping

    gene_ids = hqprobes.GENE.explode().unique().tolist()
    ensembl_ids = map_gene_ids_to_ensembl(gene_ids)
    print("✅ ENSEMBL ids list obtained", flush=True)
    
    # add ensembl ids column in hq dataset
    hqprobes['ENS_ID'] = hqprobes.GENE.map(ensembl_ids)
    # Subset df1 based on matching rows in df2 (different column names)
    subset = ner_data.merge(hqprobes, left_on=['CD', 'GP'], right_on=['CHEMBLID', 'ENS_ID'], how='inner')
    # Removing duplicated columns
    subset = subset.drop(columns=['CHEMBLID', 'ENS_ID'])
    # Writing output file
    subset.to_csv('data/3_ner_probes_triplets_ptpairs.csv', sep='\t', index=False)
    print("✅ File with NER probes data for probe-target pairs created.", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find all NER sentences from articles containing HQ probes.') 
    parser.add_argument('--input', required=True, help='Path to the file with triplets.')  # main dataset with articles triplets in data/ner_probes_triplets.csv
    parser.add_argument('--probes', required=True, help='Path to the HQ probes CSV file.') #HQ probes dictionary file in ../probesHQ/files/probesSubset.csv
    
    args = parser.parse_args()
    main(args.input, args.probes, )