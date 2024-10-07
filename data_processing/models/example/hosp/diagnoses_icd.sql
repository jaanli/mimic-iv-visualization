{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/diagnoses_icd.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/diagnoses_icd.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    CAST(seq_num AS INTEGER) AS seq_num,
    icd_code,
    CAST(icd_version AS INTEGER) AS icd_version

FROM source_data