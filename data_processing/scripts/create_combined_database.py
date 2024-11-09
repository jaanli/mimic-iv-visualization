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
    -- Get ICD-10 diagnoses codes as lists
    SELECT 
        d.subject_id,
        d.hadm_id,
        LIST(DISTINCT d.icd_code) AS diagnosis_codes
    FROM READ_PARQUET($1 || '/diagnoses_icd.parquet') d
    JOIN READ_PARQUET($1 || '/d_icd_diagnoses.parquet') ref
        ON d.icd_code = ref.icd_code
    WHERE ref.icd_version = 10
    GROUP BY d.subject_id, d.hadm_id
),
procedures AS (
    -- Get ICD-10 procedure codes as lists
    SELECT 
        p.subject_id,
        p.hadm_id,
        LIST(DISTINCT p.icd_code) AS procedure_codes
    FROM READ_PARQUET($1 || '/procedures_icd.parquet') p
    JOIN READ_PARQUET($1 || '/d_icd_procedures.parquet') ref
        ON p.icd_code = ref.icd_code
    WHERE ref.icd_version = 10
    GROUP BY p.subject_id, p.hadm_id
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
),
admissions_with_codes AS (
    -- Get admissions that have either diagnosis or procedure codes (or both)
    SELECT DISTINCT 
        COALESCE(d.subject_id, p.subject_id) as subject_id,
        COALESCE(d.hadm_id, p.hadm_id) as hadm_id
    FROM diagnoses d
    FULL OUTER JOIN procedures p
        ON d.subject_id = p.subject_id 
        AND d.hadm_id = p.hadm_id
    WHERE d.hadm_id IS NOT NULL 
        OR p.hadm_id IS NOT NULL
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
    CASE 
        WHEN d.diagnosis_codes IS NULL AND p.procedure_codes IS NOT NULL THEN p.procedure_codes
        WHEN d.diagnosis_codes IS NOT NULL AND p.procedure_codes IS NULL THEN d.diagnosis_codes
        ELSE LIST_CAT(d.diagnosis_codes, p.procedure_codes)
    END as codes,
    dn.discharge_note
FROM admission_info a
INNER JOIN admissions_with_codes ac  -- Only include admissions that have codes
    ON a.subject_id = ac.subject_id 
    AND a.hadm_id = ac.hadm_id
INNER JOIN discharge_notes dn  -- Must have discharge notes
    ON a.subject_id = dn.subject_id 
    AND a.hadm_id = dn.hadm_id
LEFT JOIN diagnoses d  -- Join back to get the actual codes
    ON a.subject_id = d.subject_id 
    AND a.hadm_id = d.hadm_id
LEFT JOIN procedures p  -- Join back to get the actual codes
    ON a.subject_id = p.subject_id 
    AND a.hadm_id = p.hadm_id
"""

# Execute query and save results
print("Creating combined patient records (ICD-10 codes only, with discharge notes and at least one code)...")
result_df = con.execute(query, [str(hosp_path), str(note_path)]).df()

# Save to parquet
output_file = output_path / 'patient_records_with_notes_icd10_complete_coded_combined.parquet'
print(f"Saving results to {output_file}")
result_df.to_parquet(str(output_file), index=False)

print("Done!")

# Close connection
con.close()