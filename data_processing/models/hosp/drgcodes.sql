{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/drgcodes.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/drgcodes.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    drg_type,
    drg_code,
    description,
    TRY_CAST(drg_severity AS INTEGER) AS drg_severity,
    TRY_CAST(drg_mortality AS INTEGER) AS drg_mortality

FROM source_data