"""Open Targets clinical-stage ordering; ranks are not numeric trial phases."""
# Source: opentargets/clinical_mining, ClinicalStageCategory.
# Preserve the indication dataset's aggregation (including its approval rules).
STAGES = (
    'UNKNOWN', 'PRECLINICAL', 'IND', 'EARLY_PHASE_1', 'PHASE_1',
    'PHASE_1_2', 'PHASE_2', 'PHASE_2_3', 'PHASE_3', 'PREAPPROVAL',
    'PHASE_4', 'APPROVAL', 'WITHDRAWAL',
)
STAGE_RANK = {stage: rank for rank, stage in enumerate(STAGES)}
RANK_STAGE = dict(enumerate(STAGES))


def stage_label(rank):
    """Notebook label: missing rank means no matched clinical indication."""
    import math
    if rank is None or math.isnan(float(rank)):
        return 'No matched indication'
    return RANK_STAGE[int(rank)].replace('_', ' ')
