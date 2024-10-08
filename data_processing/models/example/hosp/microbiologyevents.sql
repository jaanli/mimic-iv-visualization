{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/microbiologyevents.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/microbiologyevents.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip')
)

SELECT
    CAST(microevent_id AS INTEGER) AS microevent_id,
    CAST(subject_id AS INTEGER) AS subject_id,
    CAST(hadm_id AS INTEGER) AS hadm_id,
    CAST(micro_specimen_id AS INTEGER) AS micro_specimen_id,
    order_provider_id,
    TRY_CAST(chartdate AS DATE) AS chartdate,
    TRY_CAST(charttime AS TIMESTAMP) AS charttime,
    CAST(spec_itemid AS INTEGER) AS spec_itemid,
    spec_type_desc,
    CAST(test_seq AS INTEGER) AS test_seq,
    TRY_CAST(storedate AS DATE) AS storedate,
    TRY_CAST(storetime AS TIMESTAMP) AS storetime,
    CAST(test_itemid AS INTEGER) AS test_itemid,
    test_name,
    CAST(org_itemid AS INTEGER) AS org_itemid,
    org_name,
    CAST(isolate_num AS INTEGER) AS isolate_num,
    quantity,
    CAST(ab_itemid AS INTEGER) AS ab_itemid,
    ab_name,
    dilution_text,
    dilution_comparison,
    TRY_CAST(dilution_value AS FLOAT) AS dilution_value,
    interpretation,
    comments

FROM source_data