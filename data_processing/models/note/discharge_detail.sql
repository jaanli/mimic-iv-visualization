{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/note/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/note/discharge_detail.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(note_id AS VARCHAR) AS note_id,
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(field_name AS VARCHAR) AS field_name,
    CAST(field_value AS VARCHAR) AS field_value,
    CAST(field_ordinal AS INTEGER) AS field_ordinal

FROM source_data