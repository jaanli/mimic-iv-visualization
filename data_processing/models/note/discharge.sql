{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/note/{{ this.name }}.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/note/discharge.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(note_id AS VARCHAR) AS note_id,
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    CAST(note_type AS VARCHAR) AS note_type,
    CAST(note_seq AS INTEGER) AS note_seq,
    CAST(charttime AS TIMESTAMP) AS charttime,
    CAST(storetime AS TIMESTAMP) AS storetime,
    CAST(text AS VARCHAR) AS text

FROM source_data