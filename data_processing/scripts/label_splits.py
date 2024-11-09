import pandas as pd

# Load data
splits = pd.read_feather('/Users/me/data/physionet.org/processed/mimiciv/mimiciv_icd10_split.feather')
df = pd.read_parquet('~/data/physionet.org/processed/mimiciv/patient_records_with_notes_icd10_complete_coded_combined_filtered_preprocessed.parquet')

# Add split and drop index
df['split'] = df.hadm_id.map(dict(zip(splits.hadm_id, splits.split)))

# Save
df.to_parquet('~/data/physionet.org/processed/mimiciv/patient_records_with_splits.parquet', index=False)