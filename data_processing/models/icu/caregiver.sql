{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/icu/caregiver.parquet'
)}}

SELECT
    CAST(caregiver_id AS INTEGER) AS caregiver_id
FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/icu/caregiver.csv.gz',
                   header=true,
                   compression='gzip')