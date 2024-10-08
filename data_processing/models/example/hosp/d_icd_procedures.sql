{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/d_icd_procedures.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/d_icd_procedures.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    icd_code,
    CAST(icd_version AS INTEGER) AS icd_version,
    long_title

FROM source_data