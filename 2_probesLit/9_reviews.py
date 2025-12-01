### Author: Melissa F. Adasme

"""
The script uses as input the dataset data/7_ner_probes_triplets_ptpairs_dr.tsv (previously created in 7_get_drug_max_phase.py)
It takes the data and for each pmid makes a request to PubMed to retrieve the article type: primary or review. Re writes the dataset with the new column 'articleType'
"""

import requests
import time
import pandas as pd

def get_review_status_dict(pmids):
    # Configuration
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    db = "pubmed"
    retmode = "json"
    batch_size = 200
    
    # Dictionary to store results
    status_dict = {}
    
    total_ids = len(pmids)
    print(f"Starting check for {total_ids} PMIDs...")

    # Loop through batches
    for i in range(0, total_ids, batch_size):
        # 1. Prepare batch
        batch = pmids[i:i + batch_size]
        id_string = ",".join(map(str, batch))
        
        payload = {
            "db": db,
            "id": id_string,
            "retmode": retmode,
            "version": "2.0"
        }

        try:
            # 2. API Request
            response = requests.post(base_url, data=payload)
            response.raise_for_status()
            data = response.json()
            
            # 3. Process Results
            if 'result' in data:
                result_dict = data['result']
                
                # Iterate through the IDs we *requested* to ensure every ID gets a key
                for pmid in batch:
                    str_pmid = str(pmid)
                    is_review = False # Default assumption
                    
                    # Check if data exists for this ID
                    if str_pmid in result_dict:
                        article_data = result_dict[str_pmid]
                        
                        # Check pubtype list safely
                        pub_types = article_data.get('pubtype', [])
                        if "Review" in pub_types:
                            is_review = True
                    
                    # Assign to dictionary
                    status_dict[str_pmid] = is_review
            
            print(f"Processed batch {i} to {i+len(batch)}...")
            
            # 4. Rate Limiting (0.35s sleep = ~3 requests/sec max)
            time.sleep(0.35) 

        except Exception as e:
            print(f"Error processing batch starting at index {i}: {e}")
            # If a batch fails, you might want to mark them as False or None to check later
            for pmid in batch:
                if str(pmid) not in status_dict:
                    status_dict[str(pmid)] = None # or None

    return status_dict


if __name__ == "__main__":
    # Example List
    df = pd.read_csv('data/7_ner_probes_triplets_ptpairs_dr.tsv', sep="\t")
    my_ids = df.pmid.unique().tolist()
    print("✅ dataframe loaded.", flush=True)
    # print(my_ids[:5]) # Optional: print first few to check
    
    # 1. Get the dictionary
    results = get_review_status_dict(my_ids)
    print("✅ articles analysed.", flush=True)

    # 2. Process results
    # We filter specifically for True to avoid errors with None
    total_reviews = sum(1 for status in results.values() if status is True)
    
    # NEW: Count the None values (failed requests)
    total_failed = sum(1 for status in results.values() if status is None)

    # 3. Print stats
    print(f"\nTotal IDs processed: {len(results)}")
    print(f"Total Reviews: {total_reviews}")
    print(f"Total Failed/None: {total_failed}")
    
    # 4. Filter lists (Optional)
    review_ids = [pmid for pmid, status in results.items() if status is True]
    failed_ids = [pmid for pmid, status in results.items() if status is None]
    
    if len(failed_ids) > 0:
        print(f"IDs that failed: {failed_ids}")
    
    # 5. Add classification to the DataFrame
    print("\nMapping results to DataFrame...", flush=True)
    # define what the boolean values translate to
    def label_article(status):
        if status is True:
            return 'review'
        elif status is False:
            return 'primary'
        else:
            return None # Handle the None/Failed cases
    # We map the dictionary to the dataframe
    # Note: .astype(str) is crucial because your dict keys are strings
    df['articleType'] = df['pmid'].astype(str).map(results).apply(label_article)
    # 6. Verify and Preview
    print(df[['pmid', 'articleType']].head())
    # Save the updated file
    df.to_csv('data/7_ner_probes_triplets_ptpairs_dr.tsv', sep="\t", index=False)
    print("✅ New column articleType added to the dataset.", flush=True)