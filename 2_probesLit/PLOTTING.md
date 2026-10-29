# Plotting the new run alongside manuscript figures

## What is preserved

The existing `10_plotting_data.ipynb` remains the plotting notebook. Figure-generating
cells retain their dimensions, colours, fonts, palettes, bins, ranking rules, cutoffs,
axis settings, DPI and filenames. The preparation step only projects columns: it
never samples, sorts, filters or deduplicates rows. Thus repeated evidence rows
still contribute to the manuscript's row-based counters. Sentence text is retained
because several analyses distinguish sentences. The original 89 GB file is untouched.

`control`, synonym payloads and other unused columns are omitted. Diagnostic counts
for omitted columns are no longer displayed; they were not used to draw figures.
The HQ dictionary is read with only INCHI, SETID, PROBE and SYNOMS. ChEMBL phase
lookups are performed once per unique probe and mapped back to all original rows.
Scatter jitter now uses a fixed seed (0) for reproducibility. Previously it varied
between executions; dimensions and distribution are unchanged.

The full extraction/plotting path still needs validation on the actual cluster
inputs. The local tests compare row/column values, several plot aggregations and
pixel-identical rendering of the actual histogram cell on a small synthetic fixture.
They do not establish pixel equivalence of all figures with the new biological data.

## Prepare the plotting extract on the cluster

Replace the example storage, environment and partition values with your cluster settings.
After pulling the updated code, create the following private Slurm script with
`nano /project/chemical_probes/cluster/step9_plotting_data.sh`:

```bash
#!/bin/bash
#SBATCH --job-name=probes-plotdata
#SBATCH --partition=YOUR_PARTITION
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=32G
#SBATCH --time=08:00:00
set -euo pipefail
# Load your institution's Python module if required.
source /path/to/your/python-environment/bin/activate
source /project/chemical_probes/cluster/paths.sh
cd "$RUN_DIR"
python -u "$CODE_DIR/2_probesLit/9_prepare_plotting_data.py" \
  --input "$RUN_DIR/results/9_ner_probes_triplets_reviews.tsv" \
  --output "$RUN_DIR/results/plotting/plotting_data.tsv.gz" \
  --chunk-size 10000
```

Submit from a configured shell:

```bash
source /project/chemical_probes/cluster/paths.sh
sbatch --output="$RUN_DIR/logs/plotdata-%j.out" \
       --error="$RUN_DIR/logs/plotdata-%j.err" \
       /project/chemical_probes/cluster/step9_plotting_data.sh
```

Preparation reads the large TSV once in chunks and writes a gzip-compressed projection.
This is bounded by chunk size and selected row width; 32 GB is an initial allocation,
not a guarantee. Do not modify inputs while running. No steps 1–8 need rerunning.
A manifest records input size/mtime, output size, row count, selected/omitted columns,
and classification/date-coverage counts. A failed preparation does not replace the
previous extract, but its old manifest is invalidated; require both a successful
job and a fresh manifest before analysis.

For the current run expect **1,377,038 rows**. Verify:

```bash
cat "$RUN_DIR/results/plotting/plotting_data.tsv.gz.manifest.json"
ls -lh "$RUN_DIR/results/plotting/plotting_data.tsv.gz"
```

Optional: pass `--triplets "$RUN_DIR/intermediate/2_ner_probes_triplets.tsv"` if you
need the notebook's initial raw-triplet row and unique-column counts. This adds a
full scan and disk-backed exact distinct counting, which may require considerable
temporary space/time. These diagnostics do not affect any plots; without them the
notebook prints a message instead of loading the giant step 2 dataset.

## Start the notebook

The notebook now defaults to `RUN_DIR/results/plotting/plotting_data.tsv.gz`. If an
old `PROBES_ANALYSIS_FILE` variable points to the full TSV, update it explicitly.
You can inspect the compressed size before deciding whether to copy the extract
to your Mac or run Jupyter on allocated cluster resources. The notebook still loads
the projected data into pandas and creates copies; compressed size is not a RAM
estimate. Its first data-loading cell reports deep DataFrame memory, but peak
notebook usage can be several times that amount. Avoid running it on a login node.

Configure the environment before starting Jupyter (replace ChEMBL path):

```bash
export PROBES_INPUT_DIR="$INPUT_DIR"
export PROBES_RUN_DIR="$RUN_DIR"
export PROBES_FILE="$INPUT_DIR/probesSubset.csv"
export PROBES_ANALYSIS_FILE="$RUN_DIR/results/plotting/plotting_data.tsv.gz"
export CHEMBL_SQLITE="/actual/path/to/chembl_35.db"
export PROBES_REVIEW_POLICY="exclude_reviews"
cd "$CODE_DIR/2_probesLit"
jupyter lab 10_plotting_data.ipynb
```

On a Mac, set these paths to their local equivalents. For an existing Jupyter
server, restart it with the environment or assign the paths directly in the
configuration cell. Select the kernel with the plotting dependencies. Keep ChEMBL
35 for manuscript comparability; changing its release changes probe phase metadata.
The supplementary plots also require `disease_term_black_list.tsv` and
`probe_references.csv` under INPUT_DIR, as documented in the main guide.

Run all cells from the top in a fresh kernel. Existing saved outputs in the notebook
are historical and must not be mistaken for results of the new run. Save newly
executed figures separately from manuscript originals (for example, use a new run
folder for each data/review-policy combination). Current figure outputs go to the
configured run's `figures/`, tables to `tables/` and derived results to `results/`.
Archive package versions and the Git commit: matplotlib/seaborn version changes
can affect rendering even when plotting code is identical.

## Comparability and limitations that need reporting

- **Clinical stages:** the earlier migration replaced numeric target–disease phases
  with categorical clinical stages. Keep those category labels; do not relabel rank
  numbers as trial phases. The separate ChEMBL chemical-probe maxPhase analysis is
  unchanged.
- **Unknown article types:** the default `exclude_reviews` policy matches the prior
  notebook: exclude confirmed reviews, retain unknowns. It does not call unknowns
  primary. The first loading cell reports the counts. To exclude unknowns as a
  sensitivity analysis, use `PROBES_REVIEW_POLICY=classified_non_reviews` and a
  separate output run/folder. The main (all-article) figures remain unaffected.
- **Dated-evidence coverage:** existing temporal analysis remains restricted to
  available dates in the older snapshot; missing dates are not replaced with newer
  association timeseries. Retain the coverage report with manuscript comparisons.
  A positive lag is relative to available dated evidence, not proof of priority
  over every evidence source.
- **Fixed cutoffs:** the notebook's 2020–2025 and 1998 windows remain fixed, rather
  than rolling forward with the new release. Top-N entities can change with data,
  while selection logic is retained.
- **Existing category ranking:** the manuscript ranking list omits `genetic_literature`.
  Its residual `no_evidence` bucket can therefore include pairs with evidence in an
  unlisted category. This pre-existing rule is retained for comparability, not
  silently corrected. Review its interpretation before using that label in a new
  manuscript; the separate broad evidence-category plot uses a different rule.

Do not infer full reproduction from successful preparation: inspect each rendered
figure, its plotted counts, and its cohort definition before replacing manuscript
figures. The new data will legitimately change distributions and category coverage.

## Pipeline numbering and existing runs

The final execution order is **8: classify article types → 9: prepare plotting data
→ 10: plot figures**. Script and notebook renaming does not change analysis or
figure styling. The commands retain `9_ner_probes_triplets_reviews.tsv` as the
article-annotated output filename for compatibility with completed runs; its `9_`
prefix is a legacy filename, not the current step number. No data files need to be
renamed, and completed annotation jobs do not need rerunning. Reuse the existing
`pubmed_article_types.sqlite` cache.

Private Slurm scripts live outside Git, so pulling code does not update them.
Before submitting future annotation jobs, change the Python filename inside your
existing review wrapper to `8_classify_article_types.py`; name that wrapper
`step8_reviews.sh`. The preparation wrapper is now `step9_plotting_data.sh`, invoking
`9_prepare_plotting_data.py`. Open `10_plotting_data.ipynb` for the final figures.
