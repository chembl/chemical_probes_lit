"""Columns and loading policy for the manuscript plotting notebook."""
import pandas as pd
from helpers.evidence_dates import DATE_COLUMNS

# No row filtering or deduplication: repeated evidence rows affect some counters.
PLOT_COLUMNS = [
    'diseaseId', 'targetId', 'datasourceId', 'datatypeId', 'score', 'evidenceCount',
    'pmid', 'date', 'section', 'probeId', 'sentence', 'probe', 'inchiKey', 'gene',
    'allcurationYears', 'allpublicationYears', 'allstudyYears', 'datedEvidenceStatus',
    'name', 'therapeuticAreas', 'drugId', 'maxClinicalStage', 'clinicalStageRank',
    'hasClinicalIndication', 'articleType',
] + DATE_COLUMNS
PROBE_COLUMNS = ['INCHI', 'SETID', 'PROBE', 'SYNOMS']


def load_plotting_data(path):
    """Load only plotted fields, with stable identifier and numeric types."""
    columns = pd.read_csv(path, sep='\t', nrows=0).columns
    if not set(DATE_COLUMNS) <= set(columns):
        raise ValueError('Old plotting extract: rerun step 5 onward and prepare a new extract for the updated date policy. The archived manuscript notebook remains available for old data.')
    frame = pd.read_csv(path, sep='\t', usecols=PLOT_COLUMNS,
                        dtype={'pmid': str, 'probeId': str, 'targetId': str, 'diseaseId': str})
    frame['clinicalStageRank'] = pd.to_numeric(frame['clinicalStageRank'], errors='raise')
    frame['firstEvidenceYear'] = pd.to_numeric(frame['firstEvidenceYear'], errors='raise')
    return frame


def review_selection(series, policy='exclude_reviews'):
    """Preserve manuscript review exclusion by default; unknown stays explicit."""
    labels = series.fillna('').astype(str).str.strip().str.lower()
    if policy == 'exclude_reviews':
        return ~labels.eq('review')
    if policy == 'classified_non_reviews':
        return labels.eq('primary')
    raise ValueError(f'Unknown review policy: {policy}')
