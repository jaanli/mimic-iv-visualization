{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/poe_detail.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/poe_detail.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    poe_id,
    CAST(poe_seq AS INTEGER) AS poe_seq,
    CAST(subject_id AS INTEGER) AS subject_id,
    field_name,
    field_value

FROM source_data