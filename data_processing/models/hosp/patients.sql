{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/patients.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/patients.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    gender,
    CAST(anchor_age AS INTEGER) AS anchor_age,
    CAST(anchor_year AS INTEGER) AS anchor_year,
    anchor_year_group,
    TRY_CAST(dod AS DATE) AS dod

FROM source_data