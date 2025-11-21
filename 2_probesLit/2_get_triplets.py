### Author: Melissa F. Adasme

"""
The script uses as input the dataset with entities found in articles mentioning a chemical probe (previously created in 1_get_ner_hq_probes.py)
It takes the data and for each 
"""

import pandas as pd
import argparse
import itertools


def main(df_path, d_path):
    ## 1. Reading probes HQ dictionary 
    hqprobes_main = pd.read_csv(d_path, sep="\t")
    print("✅ Loaded HQ probe labels.", flush=True)

    ## 2. Reading dataset from script 1
    dataset = pd.read_csv(df_path, sep="\t")
    print("✅ Loaded OT NER results from step 1.",flush=True)
    print("Total label ids:{}".format(dataset.label_id.count()), flush=True)

    ## 3. creating probes-chembl dictionary to improve probes grounding (some ids need to be updated)
    hqprobes = hqprobes_main[['CHEMBLID', 'SYNOMS']]
    hqprobes_exp = hqprobes.assign(SYNOMS=hqprobes['SYNOMS'].str.split(',')).explode('SYNOMS')
    hqprobes_dict = dict(zip(hqprobes_exp.SYNOMS,hqprobes_exp.CHEMBLID))

    dataset['label_id'] = dataset['label'].map(hqprobes_dict).fillna(dataset['label_id'])
    print("Total labels after chembl improving:{}".format(dataset.label_id.count()), flush=True) #same number expected

    ## 4. Removing rows without ids
    dataset_id = dataset.dropna()

    ## 5. Transforming OT data into triples cooccurrencies for label ids
    # Group by 'pmid' and 'sentence' (including date and section as it is all part of the same article)
    grouped = dataset_id.groupby(['pmid', 'sentence', 'date', 'section'])
    # Extracting only groups with at least one of each entity and generating triplets. Keept writing simplified by line
    with open('data/2_ner_probes_triplets.tsv', 'w') as f:
        f.write(f"pmid\tdate\tsection\tCD\tGP\tDS\tsentence\n")
        for name,group in grouped:
            allent = group['entity_type'].unique()
            if 'CD'in allent and 'GP'in allent and 'DS'in allent:
                cd = group[group['entity_type'] == 'CD']['label_id'].unique()
                gp = group[group['entity_type'] == 'GP']['label_id'].unique()
                ds = group[group['entity_type'] == 'DS']['label_id'].unique()
                triplets = list(itertools.product(cd, gp, ds))
                for triplet in triplets:
                    f.write(f"{name[0]}\t{name[2]}\t{name[3]}\t{triplet[0]}\t{triplet[1]}\t{triplet[2]}\t{name[1]}\n") 
    print("✅ Final file with OT NER triples created.",flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse a file path.')
    parser.add_argument('-i', '--input', required=True, help='Path to the main dataset') # 1_epmc_ner_results_hq_probes_all_sent.tsv
    parser.add_argument('-d', '--probes', required=True, help='Path to HQ probes dict') #HQ probes dictionary from probesSubset.csv
    args = parser.parse_args()

    main(args.input, args.probes)