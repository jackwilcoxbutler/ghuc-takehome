-- Step 1: Mark old records as inactive
UPDATE `{bq_cur_project}.{bq_cur_dataset}.dim_patients` AS main
SET main.effective_end_date = CURRENT_DATETIME(), main.is_current = FALSE
WHERE EXISTS (
    SELECT 1
    FROM `{bq_stg_project}.{bq_stg_dataset}.dim_patients` AS staging
    WHERE main.patient_id = staging.patient_id
        AND main.is_current
        AND (
            main.first_name != staging.first_name OR
            main.last_name != staging.last_name OR
            main.date_of_birth != staging.date_of_birth OR
            main.gender != staging.gender OR
            main.address != staging.address OR
            main.city != staging.city OR
            main.state != staging.state OR
            main.zip != staging.zip OR
            main.phone != staging.phone OR
            main.insurance_id != staging.insurance_id OR
            main.insurance_effective_date != staging.insurance_effective_date
        )
);

-- Step 2: Insert new records for changes
INSERT INTO `{bq_cur_project}.{bq_cur_dataset}.dim_patients` 
(patient_id, first_name, last_name, date_of_birth, gender, address, city, state, zip, phone, insurance_id, insurance_effective_date, effective_start_date, effective_end_date, is_current)
SELECT 
    patient_id,
    first_name,
    last_name,
    date_of_birth,
    gender,
    address,
    city,
    state,
    zip,
    phone,
    insurance_id,
    insurance_effective_date,
    CURRENT_DATETIME() AS effective_start_date, 
    NULL AS effective_end_date,  -- No end date since it's the latest record
    TRUE AS is_current
FROM `{bq_stg_project}.{bq_stg_dataset}.dim_patients` AS staging
WHERE NOT EXISTS (
    SELECT 1
    FROM `{bq_cur_project}.{bq_cur_dataset}.dim_patients` AS main
    WHERE main.patient_id = staging.patient_id
        AND main.is_current
);
