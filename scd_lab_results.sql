-- Step 1: Mark old records as inactive
UPDATE `{bq_cur_project}.{bq_cur_dataset}.dim_lab_results` AS main
SET main.effective_end_date = CURRENT_DATETIME(), main.is_current = FALSE
WHERE EXISTS (
    SELECT 1
    FROM `{bq_stg_project}.{bq_stg_dataset}.dim_lab_results` AS staging
    WHERE main.lab_id = staging.lab_id
        AND main.is_current
        AND (
            main.visit_id != staging.visit_id OR
            main.test_name != staging.test_name OR
            main.test_value != staging.test_value OR
            main.test_units != staging.test_units OR
            main.reference_range != staging.reference_range OR
            main.date_performed != staging.date_performed OR
            main.date_resulted != staging.date_resulted
        )
);

-- Step 2: Insert new records for changes
INSERT INTO `{bq_cur_project}.{bq_cur_dataset}.dim_lab_results` 
(lab_id, visit_id, test_name, test_value, test_units, reference_range, date_performed, date_resulted,effective_start_date, effective_end_date, is_current)
SELECT 
    lab_id,
    visit_id, 
    test_name, 
    test_value,
    test_units,
    reference_range,
    date_performed,
    date_resulted,
    CURRENT_DATETIME() AS effective_start_date,
    effective_end_date,
    TRUE as is_current
FROM `{bq_stg_project}.{bq_stg_dataset}.dim_lab_results` AS staging
WHERE NOT EXISTS (
    SELECT 1
    FROM `{bq_cur_project}.{bq_cur_dataset}.dim_lab_results` AS main
    WHERE main.lab_id = staging.lab_id
        AND main.is_current
);
