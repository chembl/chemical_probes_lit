"""Columns and loading policy for the manuscript plotting notebook."""
import pandas as pd

# No row filtering or deduplication: repeated evidence rows affect some counters.
PLOT_COLUMNS = [
    'diseaseId', 'targetId', 'datasourceId', 'datatypeId', 'score', 'evidenceCount',
    'pmid', 'date', 'section', 'probeId', 'sentence', 'probe', 'inchiKey', 'gene',
    'allcurationYears', 'allpublicationYears', 'allstudyYears', 'datedEvidenceStatus',
    'name', 'therapeuticAreas', 'drugId', 'maxClinicalStage', 'clinicalStageRank',
    'hasClinicalIndication', 'articleType',
]
PROBE_COLUMNS = ['INCHI', 'SETID', 'PROBE', 'SYNOMS']


def load_plotting_data(path):
    """Load only plotted fields, with stable identifier and numeric types."""
    frame = pd.read_csv(path, sep='\t', usecols=PLOT_COLUMNS,
                        dtype={'pmid': str, 'probeId': str, 'targetId': str, 'diseaseId': str})
    frame['clinicalStageRank'] = pd.to_numeric(frame['clinicalStageRank'], errors='raise')
    return frame


def review_selection(series, policy='exclude_reviews'):
    """Preserve manuscript review exclusion by default; unknown stays explicit."""
    labels = series.fillna('').astype(str).str.strip().str.lower()
    if policy == 'exclude_reviews':
        return ~labels.eq('review')
    if policy == 'classified_non_reviews':
        return labels.eq('primary')
    raise ValueError(f'Unknown review policy: {policy}')
