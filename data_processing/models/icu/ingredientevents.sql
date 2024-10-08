{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/icu/ingredientevents.parquet'
)}}

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    CAST(stay_id AS INTEGER) AS stay_id,
    CAST(caregiver_id AS INTEGER) AS caregiver_id,
    TRY_CAST(starttime AS TIMESTAMP) AS starttime,
    TRY_CAST(endtime AS TIMESTAMP) AS endtime,
    TRY_CAST(storetime AS TIMESTAMP) AS storetime,
    CAST(itemid AS INTEGER) AS itemid,
    CAST(amount AS DOUBLE) AS amount,
    amountuom,
    TRY_CAST(rate AS DOUBLE) AS rate,
    rateuom,
    CAST(orderid AS INTEGER) AS orderid,
    CAST(linkorderid AS INTEGER) AS linkorderid,
    statusdescription,
    CAST(originalamount AS DOUBLE) AS originalamount,
    CAST(originalrate AS DOUBLE) AS originalrate
FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/icu/ingredientevents.csv.gz',
                   header=true,
                   compression='gzip')