{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/d_labitems.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/d_labitems.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(itemid AS INTEGER) AS itemid,
    label,
    fluid,
    category

FROM source_data