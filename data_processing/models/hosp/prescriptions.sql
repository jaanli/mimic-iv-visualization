{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/prescriptions.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/prescriptions.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    CAST(pharmacy_id AS INTEGER) AS pharmacy_id,
    poe_id,
    CAST(poe_seq AS INTEGER) AS poe_seq,
    order_provider_id,
    TRY_CAST(starttime AS TIMESTAMP) AS starttime,
    TRY_CAST(stoptime AS TIMESTAMP) AS stoptime,
    drug_type,
    drug,
    formulary_drug_cd,
    gsn,
    ndc,
    prod_strength,
    form_rx,
    TRY_CAST(dose_val_rx AS FLOAT) AS dose_val_rx,
    dose_unit_rx,
    TRY_CAST(form_val_disp AS FLOAT) AS form_val_disp,
    form_unit_disp,
    TRY_CAST(doses_per_24_hrs AS FLOAT) AS doses_per_24_hrs,
    route

FROM source_data