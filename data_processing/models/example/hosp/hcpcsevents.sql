{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/hcpcsevents.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/hcpcsevents.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    TRY_CAST(chartdate AS DATE) AS chartdate,
    hcpcs_cd,
    CAST(seq_num AS INTEGER) AS seq_num,
    short_description

FROM source_data