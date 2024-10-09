{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/figures/glucose_full.parquet'
)}}

WITH glucose_measurements AS (
    SELECT 
        l.subject_id,
        l.hadm_id,
        l.valuenum AS glucose_value,
        l.charttime AS measurement_time
    FROM 
        read_parquet('~/data/physionet.org/processed/mimiciv/hosp/labevents.parquet') l
    JOIN 
        read_parquet('~/data/physionet.org/processed/mimiciv/hosp/d_labitems.parquet') d
    ON 
        l.itemid = d.itemid
    WHERE 
        d.label = 'Glucose'
        AND l.valuenum IS NOT NULL
),
drgcodes AS (
    SELECT 
        subject_id,
        hadm_id,
        drg_type,
        drg_code
    FROM 
        read_parquet('~/data/physionet.org/processed/mimiciv/hosp/drgcodes.parquet')
    WHERE 
        drg_type = 'HCFA'
),
ms_drg_to_mdc AS (
    SELECT
        ms_drg,
        mdc
    FROM
        read_parquet('~/data/physionet.org/processed/mimiciv/hosp/ms_drg_to_mdc.parquet')
),
mdc_dictionary AS (
    SELECT
        mdc_number,
        mdc_description
    FROM
        read_parquet('~/data/physionet.org/processed/mimiciv/hosp/mdc_dictionary.parquet')
),
mdc_data AS (
    SELECT 
        g.subject_id,
        g.hadm_id,
        g.glucose_value,
        g.measurement_time,
        m.mdc,
        md.mdc_description
    FROM 
        glucose_measurements g
    JOIN 
        drgcodes d ON g.subject_id = d.subject_id AND g.hadm_id = d.hadm_id
    LEFT JOIN
        ms_drg_to_mdc m ON CAST(d.drg_code AS INTEGER) = m.ms_drg
    LEFT JOIN
        mdc_dictionary md ON m.mdc = md.mdc_number
)
SELECT 
    COUNT(*) AS count,
    mdc_description AS route,
    DATE_TRUNC('hour', measurement_time) AS time,
    CAST(FLOOR(glucose_value / 5) * 5 AS INTEGER) AS glucose
FROM 
    mdc_data
WHERE
    mdc IS NOT NULL
GROUP BY 
    mdc_description,
    DATE_TRUNC('hour', measurement_time),
    FLOOR(glucose_value / 5)
ORDER BY 
    time DESC, glucose ASC