import pandas as pd
from collections import Counter
from typing import List
import numpy as np

def filter_rare_codes(df: pd.DataFrame, min_occurrences: int = 10) -> pd.DataFrame:
    # Convert numpy arrays to lists if needed
    df['codes'] = df['codes'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
    
    code_counts = Counter()
    for code_list in df['codes']:
        if code_list is not None:
            code_counts.update(code_list)
    
    frequent_codes = {code for code, count in code_counts.items() 
                     if count >= min_occurrences}
    
    print(f"Found {len(frequent_codes)} codes that appear at least {min_occurrences} times")
    print(f"Removed {len(code_counts) - len(frequent_codes)} rare codes")
    
    def filter_code_list(codes):
        if codes is None:
            return None
        filtered = [code for code in codes if code in frequent_codes]
        return filtered if filtered else None
    
    df_filtered = df.copy()
    df_filtered['codes'] = df_filtered['codes'].apply(filter_code_list)
    
    original_len = len(df_filtered)
    df_filtered = df_filtered[df_filtered['codes'].apply(lambda x: x is not None and len(x) > 0)]
    
    print(f"Removed {original_len - len(df_filtered)} admissions that had no remaining codes")
    print(f"Final dataset has {len(df_filtered)} admissions")
    
    return df_filtered

# Read, filter and save
path = '~/data/physionet.org/processed/mimiciv/patient_records_with_notes_icd10_complete_coded_combined.parquet'
df = pd.read_parquet(path)
df_filtered = filter_rare_codes(df)
df_filtered.to_parquet(path.replace('.parquet', '_filtered.parquet'))