{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/labevents.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/labevents.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(labevent_id AS INTEGER) AS labevent_id,
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    CAST(specimen_id AS INTEGER) AS specimen_id,
    CAST(itemid AS INTEGER) AS itemid,
    order_provider_id,
    TRY_CAST(charttime AS TIMESTAMP) AS charttime,
    TRY_CAST(storetime AS TIMESTAMP) AS storetime,
    value,
    TRY_CAST(valuenum AS FLOAT) AS valuenum,
    valueuom,
    TRY_CAST(ref_range_lower AS FLOAT) AS ref_range_lower,
    TRY_CAST(ref_range_upper AS FLOAT) AS ref_range_upper,
    flag,
    priority,
    comments

FROM source_data