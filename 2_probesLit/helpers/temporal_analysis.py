"""Notebook date selection; never reconstruct record-level priority from arrays."""
import pandas as pd


def selected_years(frame):
    if 'firstEvidenceYear' not in frame:
        raise ValueError('Plotting input predates the date-policy update. Rerun step 5 onward and prepare a new plotting extract; use the archived notebook for manuscript data.')
    year = pd.to_numeric(frame['firstEvidenceYear'], errors='raise')
    invalid = year.notna() & ((year < 1000) | (year % 1 != 0))
    if invalid.any():
        raise ValueError('Invalid firstEvidenceYear in plotting input')
    return year


def add_temporal_years(frame):
    result = frame.copy()
    result['date'] = pd.to_datetime(result['date'], errors='coerce')
    result['firstEvidence'] = selected_years(result)
    result['firstEvidenceAll'] = result.groupby(['targetId', 'diseaseId'])['firstEvidence'].transform('min')
    return result
