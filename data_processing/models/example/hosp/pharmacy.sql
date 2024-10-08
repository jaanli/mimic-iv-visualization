{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/pharmacy.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/pharmacy.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip',
                                types={'fill_quantity': 'VARCHAR', 'lockout_interval': 'VARCHAR', 'one_hr_max': 'VARCHAR'})
)

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    CAST(pharmacy_id AS INTEGER) AS pharmacy_id,
    poe_id,
    TRY_CAST(starttime AS TIMESTAMP) AS starttime,
    TRY_CAST(stoptime AS TIMESTAMP) AS stoptime,
    medication,
    proc_type,
    status,
    TRY_CAST(entertime AS TIMESTAMP) AS entertime,
    TRY_CAST(verifiedtime AS TIMESTAMP) AS verifiedtime,
    route,
    frequency,
    disp_sched,
    infusion_type,
    sliding_scale,
    lockout_interval,
    basal_rate,
    one_hr_max,
    TRY_CAST(doses_per_24_hrs AS FLOAT) AS doses_per_24_hrs,
    duration,
    duration_interval,
    TRY_CAST(expiration_value AS FLOAT) AS expiration_value,
    expiration_unit,
    TRY_CAST(expirationdate AS TIMESTAMP) AS expirationdate,
    dispensation,
    TRY_CAST(fill_quantity AS FLOAT) AS fill_quantity

FROM source_data