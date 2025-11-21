# Data

This directory contains the raw and processed data for the project. The workflow consists of taking raw data from the Open Targets Platform and running it through a sequential pipeline to generate the final analysis file.

---

## Raw Data (from Open Targets)

These files are the raw inputs for the project, downloaded from the Open Targets Platform.

* `disease.parquet`: [e.g., A table of disease definitions, ids, therapeutic categories, and other metadata from Open Targets.]
* `associationByDatasourceIndirect`: 
* `probesSubset.csv`: [e.g., A specific subset of probes used for this analysis.] 
* `known_drug/`: [e.g., Directory containing data related to known drugs.]

---

## Generated Data Pipeline

The following numbered files represent the sequential processing pipeline. Each step uses the previous file as its input.
**0. `epmc_ner_results_hq_probes.tsv`**
* **Input:** `associationByDatasourceIndirect`
* **Description:** [e.g., High-quality NER (Named Entity Recognition) results for probes from Europe PMC.]

**1. `1_epmc_ner_results_hq_probes_all_sent.tsv`**
* **Input:** `epmc_ner_results_hq_probes.tsv`
* **Description:** [e.g., All sentences extracted from the raw EPMC NER results.]

**2. `2_ner_probes_triplets.tsv`**
* **Input:** `1_epmc_ner_results_hq_probes_all_sent.tsv`
* **Description:** [e.g., Triplets (subject-predicate-object) extracted from the sentences.]

**3. `3_ner_probes_triplets_ptpairs.tsv`**
* **Input:** `2_ner_probes_triplets.tsv`
* **Description:** [e.g., Triplets filtered or processed to identify probe-target pairs.]

**4. `4_ner_probes_triplets_ptpairs_ev.tsv`**
* **Input:** `3_ner_probes_triplets_ptpairs.tsv`
* **Description:** [e.g., Evidence scoring or aggregation for the pairs.]

**5. `5_ner_probes_triplets_ptpairs_evd.tsv`**
* **Input:** `4_ner_probes_triplets_ptpairs_ev.tsv`
* **Description:** [e.g., Evidence details or de-duplication step.]

**6. `6_ner_probes_triplets_ptpairs_evd_ta.tsv`**
* **Input:** `5_ner_probes_triplets_ptpairs_evd.tsv`
* **Description:** [e.g., Mapping to target associations.]

**7. `7_ner_probes_triplets_ptpairs_dr.tsv`**
* **Input:** `6_ner_probes_triplets_ptpairs_evd_ta.tsv`
* **Description:** [e.g., Final mapping to disease relations. This is the final output of the pipeline.]