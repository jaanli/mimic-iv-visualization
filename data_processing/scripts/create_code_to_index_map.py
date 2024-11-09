import pandas as pd
import json

df = pd.read_parquet('~/data/physionet.org/processed/mimiciv/patient_records_with_notes_icd10_complete_coded_combined_filtered_preprocessed.parquet')
codes = sorted(set([code for codes in df['codes'].apply(lambda x: x.tolist()) for code in codes]))
code_to_idx = {code: idx for idx, code in enumerate(codes)}

with open('code_to_index.json', 'w') as f:
    json.dump(code_to_idx, f)