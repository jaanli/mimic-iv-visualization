{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/d_hcpcs.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/d_hcpcs.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    code,
    category,
    long_description,
    short_description

FROM source_data