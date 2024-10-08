{{ config(
    materialized = 'external',
    location = '~/data/physionet.org/processed/mimiciv/hosp/emar_detail.parquet'
)}}

WITH source_data AS (
    SELECT * FROM read_csv_auto('~/data/physionet.org/files/mimiciv/3.0/hosp/emar_detail.csv.gz',
                                header=true,
                                filename=true,
                                compression='gzip', all_varchar=true)
)

SELECT
    CAST(subject_id AS INTEGER) AS subject_id,
    emar_id,
    CAST(emar_seq AS INTEGER) AS emar_seq,
    parent_field_ordinal,
    administration_type,
    pharmacy_id,
    barcode_type,
    reason_for_no_barcode,
    complete_dose_not_given,
    dose_due,
    dose_due_unit,
    dose_given,
    dose_given_unit,
    will_remainder_of_dose_be_given,
    CAST(product_amount_given AS VARCHAR) AS product_amount_given,
    product_unit,
    product_code,
    product_description,
    product_description_other,
    prior_infusion_rate,
    infusion_rate,
    infusion_rate_adjustment,
    infusion_rate_adjustment_amount,
    infusion_rate_unit,
    route,
    infusion_complete,
    completion_interval,
    new_iv_bag_hung,
    continued_infusion_in_other_location,
    restart_interval,
    side,
    site,
    non_formulary_visual_verification

FROM source_data