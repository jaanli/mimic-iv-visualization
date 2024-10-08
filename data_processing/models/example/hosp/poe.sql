{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/poe.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/poe.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    poe_id,
    CAST(poe_seq AS INTEGER) AS poe_seq,
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    TRY_CAST(ordertime AS TIMESTAMP) AS ordertime,
    order_type,
    order_subtype,
    transaction_type,
    discontinue_of_poe_id,
    discontinued_by_poe_id,
    order_provider_id,
    order_status

FROM source_data