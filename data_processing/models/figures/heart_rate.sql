{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/figures/{{  this.name }}.parquet'
)}}

WITH heart_rate_data AS (
    SELECT 
        subject_id,
        stay_id,
        charttime,
        heart_rate
    FROM read_parquet('~/data/physionet.org/figures/heart_rate_data.parquet')
),
icustays AS (
    SELECT 
        subject_id,
        stay_id,
        hadm_id
    FROM read_parquet('~/data/physionet.org/processed/mimiciv/icu/icustays.parquet')
),
drgcodes AS (
    SELECT 
        subject_id,
        hadm_id,
        drg_type,
        drg_code
    FROM read_parquet('~/data/physionet.org/processed/mimiciv/hosp/drgcodes.parquet')
    WHERE drg_type = 'HCFA'
),
ms_drg_to_mdc AS (
    SELECT
        ms_drg,
        mdc
    FROM read_parquet('~/data/physionet.org/processed/mimiciv/hosp/ms_drg_to_mdc.parquet')
)
SELECT 
    h.subject_id,
    h.stay_id,
    h.charttime,
    h.heart_rate,
    m.mdc
FROM 
    heart_rate_data h
JOIN 
    icustays i ON h.stay_id = i.stay_id
JOIN 
    drgcodes d ON i.subject_id = d.subject_id AND i.hadm_id = d.hadm_id
LEFT JOIN
    ms_drg_to_mdc m ON CAST(d.drg_code AS INTEGER) = m.ms_drg
WHERE
    m.mdc IS NOT NULL
ORDER BY 
    h.subject_id, h.stay_id, h.charttime