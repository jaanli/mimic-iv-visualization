import duckdb
import pandas as pd
from pathlib import Path

# Define base paths
base_path = Path('~/data/physionet.org/processed/mimiciv')
hosp_path = base_path / 'hosp'
note_path = base_path / 'note'
output_path = base_path

# Ensure output directory exists
output_path.mkdir(parents=True, exist_ok=True)

# Initialize DuckDB connection
con = duckdb.connect()

# Create the combined dataset
query = """
WITH diagnoses AS (
    -- Get diagnoses codes as lists
    SELECT 
        subject_id,
        hadm_id,
        LIST(DISTINCT icd_code) AS diagnosis_codes
    FROM READ_PARQUET($1 || '/diagnoses_icd.parquet')
    GROUP BY subject_id, hadm_id
),
procedures AS (
    -- Get procedure codes as lists
    SELECT 
        subject_id,
        hadm_id,
        LIST(DISTINCT icd_code) AS procedure_codes
    FROM READ_PARQUET($1 || '/procedures_icd.parquet')
    GROUP BY subject_id, hadm_id
),
admission_info AS (
    -- Get basic admission information
    SELECT 
        subject_id,
        hadm_id,
        admittime,
        dischtime,
        admission_type,
        admission_location,
        discharge_location,
        insurance
    FROM READ_PARQUET($1 || '/admissions.parquet')
),
discharge_notes AS (
    -- Get discharge notes
    SELECT 
        subject_id,
        hadm_id,
        charttime,
        text as discharge_note
    FROM READ_PARQUET($2 || '/discharge.parquet')
    WHERE note_type = 'DS'
)

SELECT 
    a.subject_id,
    a.hadm_id,
    a.admittime,
    a.dischtime,
    a.admission_type,
    a.admission_location,
    a.discharge_location,
    a.insurance,
    d.diagnosis_codes,
    p.procedure_codes,
    dn.discharge_note
FROM admission_info a
LEFT JOIN diagnoses d
    ON a.subject_id = d.subject_id 
    AND a.hadm_id = d.hadm_id
LEFT JOIN procedures p
    ON a.subject_id = p.subject_id 
    AND a.hadm_id = p.hadm_id
LEFT JOIN discharge_notes dn
    ON a.subject_id = dn.subject_id 
    AND a.hadm_id = dn.hadm_id
"""

# Execute query and save results
print("Creating combined patient records...")
result_df = con.execute(query, [str(hosp_path), str(note_path)]).df()

# Save to parquet
output_file = output_path / 'patient_records_with_notes.parquet'
print(f"Saving results to {output_file}")
result_df.to_parquet(str(output_file), index=False)

print("Done!")

# Close connection
con.close()