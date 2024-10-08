{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/provider.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/provider.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    provider_id

FROM source_data