{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/emar.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/emar.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    emar_id,
    CAST(emar_seq AS INTEGER) AS emar_seq,
    poe_id,
    pharmacy_id,
    enter_provider_id,
    TRY_CAST(charttime AS TIMESTAMP) AS charttime,
    medication,
    event_txt,
    TRY_CAST(scheduletime AS TIMESTAMP) AS scheduletime,
    TRY_CAST(storetime AS TIMESTAMP) AS storetime

FROM source_data