{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/icu/d_items.parquet'
)}}

SELECT
    CAST(itemid AS INTEGER) AS itemid,
    label,
    abbreviation,
    linksto,
    category,
    unitname,
    param_type,
    TRY_CAST(lownormalvalue AS DOUBLE) AS lownormalvalue,
    TRY_CAST(highnormalvalue AS DOUBLE) AS highnormalvalue
FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/icu/d_items.csv.gz',
                   header=true,
                   compression='gzip')