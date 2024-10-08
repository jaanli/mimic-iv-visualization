{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/omr.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/omr.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    TRY_CAST(chartdate AS DATE) AS chartdate,
    CAST(seq_num AS INTEGER) AS seq_num,
    result_name,
    result_value

FROM source_data