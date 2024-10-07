{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/admissions.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/admissions.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    TRY_CAST(admittime AS TIMESTAMP) AS admittime,
    TRY_CAST(dischtime AS TIMESTAMP) AS dischtime,
    TRY_CAST(deathtime AS TIMESTAMP) AS deathtime,
    admission_type,
    admit_provider_id,
    admission_location,
    discharge_location,
    insurance,
    language,
    marital_status,
    race,
    TRY_CAST(edregtime AS TIMESTAMP) AS edregtime,
    TRY_CAST(edouttime AS TIMESTAMP) AS edouttime,
    CAST(hospital_expire_flag AS BOOLEAN) AS hospital_expire_flag

FROM source_data