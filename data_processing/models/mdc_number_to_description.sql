{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/mdc_dictionary.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_json_auto('~/projects/mimic-iv-visualization/data/mdc_dictionary.json')
)

SELECT
    CAST(mdc_number AS INTEGER) AS mdc_number,
    mdc_description
FROM source_data