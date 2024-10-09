{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/ms_drg_to_mdc.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_json_auto('~/projects/mimic-iv-visualization/data/ms_drg_to_mdc.json')
)

SELECT
    CAST(ms_drg AS INTEGER) AS ms_drg,
    CAST(mdc AS INTEGER) AS mdc
FROM source_data