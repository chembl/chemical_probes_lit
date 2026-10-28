# My cluster execution notes

This checklist covers input preparation and downloading Open Targets data with
script 0. It assumes the project folders, Python environment, `cluster/paths.sh`
and `cluster/step0_download.sh` already exist. Steps 1–9 are documented in
[the pipeline execution guide](2_probesLit/README.md).

## Locations

- Public code checkout: `/nfs/production/arl/chembl/madasme/projects/chemical_probes/code`
- Private scripts: `/nfs/production/arl/chembl/madasme/projects/chemical_probes/cluster`
- Inputs: `/nfs/production/arl/chembl/madasme/projects/chemical_probes/inputs`
- Runs: `/nfs/production/arl/chembl/madasme/projects/chemical_probes/runs`
- Environment activation: `source /hps/software/users/chembl/adasme/cprobeslit_venv/bin/activate`

These notes contain institutional paths. Keep this file local/private unless you
intend to publish those details. Do not include it in a public commit by accident.

## 1. Check releases before EVERY new execution

Do not assume the previous release is still the latest. Before downloading:

1. Inspect the Open Targets release directories at
   <https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/>.
2. Choose the intended release and confirm it contains all required directories:
   `output/association_by_datasource_indirect/`, `output/disease/`, and
   `output/clinical_target/` and `output/clinical_indication/`.
3. Separately check the literature directory:
   <https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/latest/intermediate/literature/match/>.
   Prefer a fixed literature release if the corresponding directory is available.
4. Record the HQ dictionary version and dated-evidence version too. Those inputs
   are prepared separately; script 0 does not download them.
5. Check the release arguments in the saved job script before submission.

For the current execution, the intended output release is **26.09** and the
literature argument is **latest**. This is a selected configuration, not a claim
that 26.09 will remain the latest or that upstream availability was verified here.

`--release 26.09` controls association, disease and clinical data only.
`--literature-release latest` independently selects the changing literature data.
To pin literature as well, use `--literature-release 26.09` only after confirming
that its literature directory exists. If the layout differs, the downloader also
supports `--literature-dir` with the complete remote server directory.

The Python downloader now defaults to `26.09`. Always supply the intended
`--release` explicitly in the job script; editing the private job does not change
the Python default.

## 2. Choose a new snapshot and run

Use a fresh download directory for each new snapshot, even when downloading the
same release again. Do not mix new files with a previous or partial download.

Example names for this configuration:

- Open Targets snapshot: `26.09_snapshot_001`
- Run: `analysis_001` for the current setup; use a new name for later analyses.

Do not change shared `paths.sh` while dependent jobs are queued or running: jobs
read it when they execute. Keep a private copy of the configuration and job script
used for each run.

## 3. Copy the HQ dictionary from the Mac

Run in the **Mac terminal**, changing the destination version label as needed:

```bash
scp -C "/Users/adasme/Documents/codes/chemical_probes_lit/3_probesHQ/files/probesSubset.csv" \
  adasme@codon-login:/nfs/production/arl/chembl/madasme/projects/chemical_probes/inputs/probesSubset_2026_10_27.csv
```

Wait for successful completion. The file uploaded during the initial setup was
saved as `inputs/probesSubset.csv` and was approximately 333 MB. Reuse that path
for the current run; versioned filenames help preserve inputs on future runs.

## 4. Prepare dated evidence

The current evidence is already at:

```text
/nfs/production/arl/chembl/madasme/projects/chemical_probes/inputs/evidenceDated
```

No copying is needed when reusing this dataset. For a new version, copy its
extracted directory to a new destination (replace the source path and version):

```bash
cp -r /actual/path/to/new/evidenceDated \
  /nfs/production/arl/chembl/madasme/projects/chemical_probes/inputs/evidenceDated_NEW_VERSION
```

Use a destination that does not already exist. It should contain parquet source
folders such as `sourceId=chembl` and `sourceId=europepmc`.

## 5. Update the private path configuration

Run on the **cluster**:

```bash
nano /nfs/production/arl/chembl/madasme/projects/chemical_probes/cluster/paths.sh
```

Edit the existing entries rather than appending duplicates. Current setup:

```bash
export CODE_DIR="/nfs/production/arl/chembl/madasme/projects/chemical_probes/code"
export INPUT_DIR="/nfs/production/arl/chembl/madasme/projects/chemical_probes/inputs"
export OT_DIR="$INPUT_DIR/opentargets/26.09_snapshot_001"
export RUN_DIR="/nfs/production/arl/chembl/madasme/projects/chemical_probes/runs/analysis_001"
export PROBES_FILE="$INPUT_DIR/probesSubset.csv"
export DATED_EVIDENCE_DIR="$INPUT_DIR/evidenceDated"
```

For later runs, update the snapshot, run and input filenames to the versions chosen
above. If step 3 uploaded a versioned dictionary, set `PROBES_FILE` to that filename.
Save with Ctrl+O, Enter, then exit with Ctrl+X.

Load and check:

```bash
source /nfs/production/arl/chembl/madasme/projects/chemical_probes/cluster/paths.sh
printf 'Code: %s\nDownloads: %s\nRun: %s\n' "$CODE_DIR" "$OT_DIR" "$RUN_DIR"
ls -lh "$PROBES_FILE"
ls "$DATED_EVIDENCE_DIR"
```

Create only the directories needed for the selected snapshot/run:

```bash
mkdir -p "$OT_DIR"
mkdir -p "$RUN_DIR"/{intermediate,results,figures,tables,logs}
```

## 6. Check the code and environment

```bash
source /hps/software/users/chembl/adasme/cprobeslit_venv/bin/activate
python "$CODE_DIR/2_probesLit/0_download_OT_files.py" --help
```

This displays arguments without downloading data. If dependencies fail to import
in an actual job, inspect its error log and the modules loaded by the job script.
Keep the code checkout unchanged while its jobs are queued or running.

## 7. Edit and verify the saved download job

```bash
nano /nfs/production/arl/chembl/madasme/projects/chemical_probes/cluster/step0_download.sh
```

The current job file should contain:

```bash
#!/bin/bash
#SBATCH --time=8:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --nodes=1
#SBATCH --partition=research
#SBATCH --mem=8G
#SBATCH --job-name=probes-download
#SBATCH --output=download-%j.out
#SBATCH --error=download-%j.err
#SBATCH --mail-user=adasme@ebi.ac.uk
#SBATCH --mail-type=END,FAIL

set -euo pipefail

module load python/3.11.7
module load py-psutil/5.9.5
module load py-pyspark/3.3.1

source /hps/software/users/chembl/adasme/cprobeslit_venv/bin/activate
source /nfs/production/arl/chembl/madasme/projects/chemical_probes/cluster/paths.sh

cd "$RUN_DIR"

python -u "$CODE_DIR/2_probesLit/0_download_OT_files.py" \
    --output-dir "$OT_DIR" \
    --release 26.09 \
    --literature-release latest
```

Update release arguments for every new execution according to step 1. Save the
file, then inspect it:

```bash
cat /nfs/production/arl/chembl/madasme/projects/chemical_probes/cluster/step0_download.sh
```

Editing a `.sh` file does not run it and does not change an already-submitted job.
The resources above are for downloading, not the large Spark analysis steps.
Compute-node access to the EBI HTTPS server is required.

## 8. If replacing an incorrect download job

Skip this section for a normal new run. If an old job used the wrong release:

```bash
squeue -u adasme
```

Cancel only that job, replacing `JOB_ID` with its actual number:

```bash
scancel JOB_ID
```

Check that it has stopped before reusing any destination. Prefer a fresh snapshot
folder regardless: the downloader does not remove stale files or resume partial
files. Update `OT_DIR` in `paths.sh`, then source it again. Do not delete the old
snapshot until you are sure it is no longer needed.

## 9. Save the configuration and submit

```bash
cp /nfs/production/arl/chembl/madasme/projects/chemical_probes/cluster/paths.sh \
  "$RUN_DIR/paths.sh"
cp /nfs/production/arl/chembl/madasme/projects/chemical_probes/cluster/step0_download.sh \
  "$RUN_DIR/step0_download.sh"
git -C "$CODE_DIR" rev-parse HEAD > "$RUN_DIR/code_commit.txt"
python -m pip freeze > "$RUN_DIR/python_packages.txt"
```

These copies record the settings; the submitted job below still reads the live
`cluster/paths.sh`. Package versions describe the active shell environment; job
modules can affect its effective environment.

Submit the saved private script:

```bash
sbatch \
  --output="$RUN_DIR/logs/download-%j.out" \
  --error="$RUN_DIR/logs/download-%j.err" \
  /nfs/production/arl/chembl/madasme/projects/chemical_probes/cluster/step0_download.sh
```

Record the returned job ID. Command-line log paths override those in the script.
Submitting with `sbatch` queues the job; it does not execute the analysis directly
on the login node.

## 10. Monitor progress

```bash
squeue -u adasme
```

Replace `JOB_ID` with the submitted job number:

```bash
tail -n 20 "$RUN_DIR/logs/download-JOB_ID.out"
tail -n 20 "$RUN_DIR/logs/download-JOB_ID.err"
```

`R` means running; `PD` means pending. The downloader logs the start of each dataset,
not a percentage for every file, so a quiet log does not necessarily mean failure.

## 11. Confirm completion

After the job leaves the queue:

```bash
sacct -j JOB_ID --format=JobID,State,ExitCode,Elapsed
```

Look for `COMPLETED` and exit code `0:0`. Inspect any errors before proceeding.

```bash
ls -lh "$OT_DIR"
cat "$OT_DIR/download_manifest.json"
```

Expected contents:

```text
literature_match/
association_by_datasource_indirect/
disease/
clinical_target/
clinical_indication/
download_manifest.json
```

The manifest is written after all five downloads succeed. Confirm its `release`
is the selected release (26.09 for this run). It records requested releases,
source URLs and completion time. It does not resolve `latest` to a fixed release
or calculate checksums. Preserve the input snapshot for reproducibility.

## 12. Before starting step 1

The public literature schema has been inspected and `mappedId` is the selected
entity identifier. Updated step 1 requires these parquet fields:

```text
pmid, label, mappedId, type, pubDate, section, text
```

Triplet extraction expects entity types `CD`, `GP` and `DS`. Public literature
will be read from `$OT_DIR/literature_match`; `failedMatches` is not used.
Changing releases may change results even if the scripts execute successfully.
Record the dated-evidence version separately from the newer Open Targets release.

Once the data is validated, follow the step 1–9 commands in
[the execution guide](2_probesLit/README.md), adapting the private Slurm resource
requests for each step. Do not use the download job's 8 GB allocation for steps
that configure a 200 GB Spark heap.

## Updated clinical-data workflow

After updating the cluster code checkout, step 0 uses HTTPS and checks all five
listings before downloading. It now downloads `clinical_target` and
`clinical_indication`, replacing retired `known_drug`. No `clinical_report` download
is needed. Validate availability with the environment activated:

```bash
python "$CODE_DIR/2_probesLit/0_download_OT_files.py" \
    --output-dir "$OT_DIR" --release 26.09 --literature-release latest --check-only
```

The existing step 0 Slurm command still works with explicit release 26.09. Update
your private step 7 Slurm command to:

```bash
python "$CODE_DIR/2_probesLit/7_get_drug_max_phase.py" \
    --input "$RUN_DIR/intermediate/6_ner_probes_triplets_ptpairs_evd_ta.tsv" \
    --clinical-target "$OT_DIR/clinical_target" \
    --clinical-indication "$OT_DIR/clinical_indication" \
    --output "$RUN_DIR/results/7_ner_probes_triplets_ptpairs_dr.tsv"
```

Use appropriate Spark resources for step 7. Re-run step 9 afterward, then the
updated plotting notebook. Output now contains `maxClinicalStage`,
`clinicalStageRank`, and `hasClinicalIndication` instead of `phase` and `status`.
Ranks are not phases; UNKNOWN is distinct from no matched indication. The actual
26.09 parquet schemas still need cluster validation. See the execution guide's
clinical migration section for scientific interpretation and ordering.

### Literature identifier update

Step 1 now reads `mappedId` and writes it as `label_id`; the remaining steps keep
the same TSV interface. Update the required-column list in your private
`cluster/check_inputs.sh` from `keywordId` to `mappedId`, then rerun that validation
job so it can continue to the clinical schemas. Updating the repository does not
edit that private shell script. No additional mapping/validity filter was added.

## TSV parsing fix: regenerate step 1 before retrying step 2

The original Spark TSV dialect disagreed with pandas on quote escaping. A global
backslash escape is not a safe workaround: labels such as `NF-\` and LaTeX sentences
contain literal backslashes. Updated code uses doubled quotes consistently, preserves
backslashes and whitespace, and supports multiline fields in Spark readers. Step 2
also writes properly quoted TSV instead of concatenating raw sentence text.

After committing/pushing and pulling the updated code on the cluster, rerun the
existing step 1 job to regenerate BOTH TSV outputs from the original downloaded
parquet. Wait for successful completion, then rerun step 2. No downloads need to be
repeated. Do not skip bad rows or add `escapechar` to pandas. Existing same-named
step 1 outputs will be replaced; copy them elsewhere first if retaining the old
exports is useful. Spark round-trip tests require the cluster Java/PySpark environment.

## Association migration: rerun from step 4

After pulling the updated code, reuse the existing step 4 Slurm script. Inputs from
steps 1–3 are reusable. Step 4 handles aggregationType/aggregationValue and restores
datatypeId using the documented public OT mapping (not a verified release-tagged
mapping). Unknown sources fail rather than being guessed.

Step 5 keeps exact source IDs: newer clinical_precedence/gwas_credible_sets are not
renamed to older chembl/ot_genetics_portal. After step 5, inspect:

```bash
cat "$RUN_DIR/intermediate/5_ner_probes_triplets_ptpairs_evd.tsv.dated_coverage.json"
```

Missing dates stay null. The new datedEvidenceStatus distinguishes source_absent,
pair_absent, dates_missing, no_association and matched. The older dated snapshot
provides incomplete coverage of current associations; interpret temporal results
only within that coverage. Neither missing dates nor missing source matches imply
absence of historical evidence. Existing step 4/5 job arguments are unchanged.

## Script 9: chunked article-type annotation

After updating the cluster checkout, run script 9 using the existing environment:

```bash
python -u "$CODE_DIR/2_probesLit/9_reviews.py" \
    --input "$RUN_DIR/results/7_ner_probes_triplets_ptpairs_dr.tsv" \
    --output "$RUN_DIR/results/9_ner_probes_triplets_reviews.tsv" \
    --cache "$RUN_DIR/results/pubmed_article_types.sqlite" \
    --chunk-size 10000
```

Put this command in a private Slurm job; start with 32 GB memory and network access.
It does not require Spark. Two passes through the TSV are expected. Successful
PubMed lookups persist in SQLite; restarting with the same cache only queries
unresolved/new IDs. Missing results remain blank rather than being marked primary.
Check the completion log for unresolved counts before review-exclusion analysis.
Do not rerun steps 1–7. Allow space for another roughly input-sized TSV. Output is
published after all chunks are written; writing restarts after interruption, but
cached API work is retained. The notebook's own large-file memory needs remain.
