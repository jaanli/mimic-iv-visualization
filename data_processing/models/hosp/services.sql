{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/services.parquet'
)}}

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    TRY_CAST(transfertime AS TIMESTAMP) AS transfertime,
    prev_service,
    curr_service
FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/services.csv.gz',
                   header=true,
                   compression='gzip')