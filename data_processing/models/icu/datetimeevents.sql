{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/icu/datetimeevents.parquet'
)}}

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    CAST(stay_id AS INTEGER) AS stay_id,
    CAST(caregiver_id AS INTEGER) AS caregiver_id,
    TRY_CAST(charttime AS TIMESTAMP) AS charttime,
    TRY_CAST(storetime AS TIMESTAMP) AS storetime,
    CAST(itemid AS INTEGER) AS itemid,
    TRY_CAST(value AS TIMESTAMP) AS value,
    valueuom,
    CAST(warning AS BOOLEAN) AS warning
FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/icu/datetimeevents.csv.gz',
                   header=true,
                   compression='gzip')