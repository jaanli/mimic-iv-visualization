{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/transfers.parquet'
)}}

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    CAST(transfer_id AS INTEGER) AS transfer_id,
    eventtype,
    careunit,
    TRY_CAST(intime AS TIMESTAMP) AS intime,
    TRY_CAST(outtime AS TIMESTAMP) AS outtime
FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/transfers.csv.gz',
                   header=true,
                   compression='gzip')