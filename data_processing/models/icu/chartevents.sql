{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/icu/chartevents.parquet'
)}}

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    CAST(stay_id AS INTEGER) AS stay_id,
    CAST(caregiver_id AS INTEGER) AS caregiver_id,
    TRY_CAST(charttime AS TIMESTAMP) AS charttime,
    TRY_CAST(storetime AS TIMESTAMP) AS storetime,
    CAST(itemid AS INTEGER) AS itemid,
    value,
    TRY_CAST(valuenum AS DOUBLE) AS valuenum,
    valueuom,
    CAST(warning AS BOOLEAN) AS warning
FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/icu/chartevents.csv.gz',
                   header=true,
                   compression='gzip')