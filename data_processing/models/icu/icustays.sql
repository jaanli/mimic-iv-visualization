{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/icu/icustays.parquet'
)}}

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    CAST(stay_id AS INTEGER) AS stay_id,
    first_careunit,
    last_careunit,
    TRY_CAST(intime AS TIMESTAMP) AS intime,
    TRY_CAST(outtime AS TIMESTAMP) AS outtime,
    CAST(los AS DOUBLE) AS los
FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/icu/icustays.csv.gz',
                   header=true,
                   compression='gzip')