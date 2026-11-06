# Chemical Probes in Scientific Literature: A Pipeline for Target-Disease Evidence

This repository contains all the code and scripts necessary to reproduce the analysis presented in the manuscript: "Chemical Probes in Scientific Literature: Expanding and Validating Target-Disease Evidence." Preprint [here](https://www.biorxiv.org/content/10.64898/2026.02.19.706919v1)

## Abstract

This project leverages Natural Language Processing (NLP) to systematically analyse open-access scientific literature for evidence of novel target-disease (T-D) associations mediated by chemical probes. The pipeline identifies chemical probe-target-disease (P-T-D) triplets, quantifies their prevalence, and performs a temporal analysis to determine cases where probe-based evidence predates other non-literature evidence streams in platforms like Open Targets. The goal is to highlight the value of chemical probe literature as a resource for early-stage target validation and hypothesis generation in drug discovery.

## Project structure

The numbered scripts in `2_probesLit/` run the systematic pipeline. Build or select
its probe dictionary using `3_probesHQ/` first, then execute steps 0–9 and use the
step 10 notebook for plotting. Shared code is grouped under `helpers/`.

```text
chemical_probes_lit/
├── 1_pilot/                                  # Original pilot study
│   ├── data/                                 # Local pilot inputs and results
│   ├── figs/                                 # Local pilot figures
│   ├── databases.py                          # Pilot database utilities
│   └── note_ProbesDataset.ipynb               # Pilot extraction and analysis
│
├── 2_probesLit/                              # Systematic literature pipeline
│   ├── 0_download_OT_files.py                 # Download Open Targets inputs
│   ├── 1_get_ner_hq_probes.py                 # Select articles mentioning dictionary probes
│   ├── 2_get_triplets.py                      # Extract sentence-level chemical–target–disease triples
│   ├── 3_filter_targets.py                    # Retain dictionary-supported probe–target matches
│   ├── 4_get_OT_evidence.py                   # Link Open Targets association evidence
│   ├── 5_get_OT_dated_evidence.py             # Add release-matched evidence dates
│   ├── 6_map_disease_therapeutic_area.py       # Add disease names and therapeutic areas
│   ├── 7_get_drug_max_phase.py                # Add disease-specific clinical stages
│   ├── 8_classify_article_types.py            # Classify PubMed article types
│   ├── 9_prepare_plotting_data.py             # Prepare the compact plotting extract
│   ├── 10_plotting_data.ipynb                 # Generate figures and supplementary tables
│   ├── helpers/
│   │   ├── association_schema.py             # Association schema and datasource mappings
│   │   ├── clinical_stages.py                # Clinical-stage ordering and labels
│   │   ├── evidence_dates.py                 # Date selection and coverage reporting
│   │   ├── ot_download.py                    # Open Targets download utilities
│   │   ├── pipeline_paths.py                 # Output-path handling
│   │   ├── plotting_data.py                  # Plotting columns, loading and review selection
│   │   ├── probe_dictionary.py               # Probe identifiers and synonym grounding
│   │   ├── supplement_probe_dictionary.py    # Build a documented dictionary supplement
│   │   ├── temporal_analysis.py              # Temporal-analysis calculations
│   │   ├── tools.py                          # Shared database and file utilities
│   │   └── tsv_format.py                     # Consistent TSV reading and writing
│   └── README.md                             # Pipeline commands and analysis methods
│
├── 3_probesHQ/                               # Probe-dictionary construction
│   ├── files/                                # Local source exports and database inputs
│   ├── probes_hq_dataset.ipynb                # Assemble and filter the probe dictionary
│   ├── tools.py                              # Dictionary database utilities
│   └── README.md                             # Dictionary preparation instructions
│
├── tests/                                    # Pipeline and helper regression tests
├── .gitignore                                # Exclude local data, figures and private files
├── LICENCE
├── requirements.txt                          # Python dependencies
└── README.md                                 # Project overview
```

Systematic-pipeline inputs and outputs are stored **outside the repository**, with
separate `inputs/` and run-specific `intermediate/`, `results/`, `figures/`, `tables/`
and `logs/` folders. Private Slurm scripts and configuration also stay outside the
checkout. The pilot retains its original local `data/` and `figs/` layout; local
data and figure files are generally excluded by `.gitignore`.

See the [pipeline guide](2_probesLit/README.md) for execution commands and the
[dictionary guide](3_probesHQ/README.md) for input preparation.

## Data Availability
We are committed to transparency and reproducibility. This section outlines the availability of the data for both the main systematic pipeline and the preliminary pilot study.

#### Systematic Pipeline

The main systematic pipeline is fully reproducible. All scripts and code required to run the analysis are available in this GitLab repository. The public input files are detailed in the Input Files section.
Due to their size, the intermediate and final datasets generated by the systematic pipeline are not stored on GitLab. They are publicly available for download from our dedicated repository at: [Insert Zenodo/Figshare/Repository URL here].

#### Pilot Study

The pilot study served as an initial proof-of-concept. To support our findings, all code, intermediate data, final results, figures, and plots from this preliminary analysis are publicly available directly in the 1_pilot/ directory of this repository.
Please note that the initial, dictionary-based annotated dataset used as the starting point for the pilot is not included due to its legacy format. However, the most valuable output of this pilot work—the manually curated bioactivity data—has been formally integrated into the public domain and is now available as part of the ChEMBL 36 database release.

## Systematic Pipeline Execution

### Prerequisites
#### Input files
1. Annotated Literature

This is the primary dataset from the EuropePMC NER pipeline, containing entity matches from scientific articles. The analysis focuses exclusively on successful "matches"—data where entities have been grounded to a standard Open Targets identifier. 

    - Public Access: The required successful matches are available on the Open Targets HTTPS server
    https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/latest/intermediate/literature/match/

2. Open Targets Evidence

This dataset contains all existing target-disease association evidence curated by Open Targets.  

    - Public Access: http://ftp.ebi.ac.uk/pub/databases/opentargets/platform/26.09/output/association_by_datasource_indirect

3. Dated Open Targets Evidence

    **Current pipeline:** step 5 uses release-matched association time series already
    downloaded in step 0. The older dataset below is an optional historical input.
    See [date rules and migration](2_probesLit/README.md#temporal-evidence-dates).

This dataset provides time stamps for the evidence, which is crucial for the temporal analysis.

    - Public Access: A public version is available at Zenodo: https://zenodo.org/records/15922783

4. Therapeutic area (parent ontology id) for the disease term assigned by Open Targets.
    - Public access: http://ftp.ebi.ac.uk/pub/databases/opentargets/platform/26.09/output/disease

5. Disease-specific clinical stages of drugs linked to T-D pairs (Clinical Target + Clinical Indication).
    - Public access: https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/26.09/output/clinical_target/ and https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/26.09/output/clinical_indication/

#### ChEMBL database
This project requires a local copy of the ChEMBL database (version 35) to extract chemical probe synonyms and other relevant metadata.

- Navigate to the ChEMBL 35 release page on the EBI HTTPS server:
```
https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_35/
```
- Download the MySQL dump file: chembl_35_mysql.tar.gz.
- Decompress the .tar.gz file in a local directory of your choice.
- Configure the path in the notebooks: You must provide the local file path to the SQL dump in the following two Jupyter notebooks:
```
1_pilot/note_ProbesDataset.ipynb
3_probesHQ/probes_hq_dataset.ipynb
```
In each notebook, find the configuration cell near the top and update the variable `chemblpath` for the ChEMBL path with the location of the file on your system.

#### Python 3.11.7
The historical manuscript analysis used Python 3.11.7. The current dependency pins differ from that historical environment; validate your Python, Java and Spark versions together before a production run.

#### PySpark 3.3.1
The historical manuscript environment used PySpark 3.3.1. The current requirements.txt pins PySpark 4.1.2; use a validated Python/Java/Spark environment for your cluster. See the execution guide for this distinction.

#### Computational Resources
This pipeline is designed to process large-scale literature and evidence datasets. While PySpark is utilised for its efficiency, the computational and memory requirements remain substantial. Running the full analysis on a standard local machine is likely to be very slow or may fail due to insufficient memory.

For a timely and successful execution, we strongly recommend setting up the environment and running the scripts on a high-performance computing (HPC) cluster.

To provide a benchmark, the analysis for the manuscript was performed using the following resources. Users should aim for a comparable environment:

- Memory (RAM): The PySpark steps are the most memory-intensive stages of the pipeline. A minimum of 250GB is recommended for the script in step 1 (1_get_ner_hq_probes.py). 
- CPU Cores: The pipeline was executed using 24 cores for script 1 and 6 for all others. Performance will scale with the number of available cores, particularly for the PySpark jobs.
- Total Runtime: The full systematic pipeline took approximately 1.5 hours to complete on the hardware specified above

### Installation
1. Clone the repository in your preferred local path

```
git clone https://gitlab.ebi.ac.uk/chembl/research/chemical_probes_lit.git
cd chemical_probes_lit
ls
```
You should be able to see the project content.

2. Create the python enviroment

It is recommended to create the python enviroment outside the git project. Go to your preferred path and create the enviroment.
```
cd your_path
python -m venv cprobesenv
```

3. Activate the enviroment 

Mac or Linux:
```
source cprobesenv/bin/activate
```
Windows:
```
.\cprobesenv\Scripts\activate.bat
```
4. Install the required packages using the requirements.txt file
```
pip install -r requirements.txt
```
### Execution

The main pipeline uses **explicit external input and output paths**. Keep your
institutional Slurm scripts in an ordinary private folder outside this checkout;
a second repository is not required.

See **[the complete execution guide](2_probesLit/README.md)** for:

- External storage layout and environment setup.
- Copyable commands for downloading inputs and running steps 1–7.
- Article-type annotation and plotting with external result directories.
- A private Slurm job example, logging, job dependencies and migration instructions.

Every analysis script requires `--output`; the downloader requires `--output-dir`.
Old job commands that rely on writing to `data/` must be updated. The plotting
notebook uses a configuration cell for external input, result, figure and table paths.


## Licence
This project is licensed under the MIT License. This means you are free to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the software for any purpose, including commercial use.

For the full license text, please see the `LICENSE` file in this repository.

## Citation
