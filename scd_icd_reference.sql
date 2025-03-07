-- Step 1: Mark old records as inactive
UPDATE `{bq_cur_project}.{bq_cur_dataset}.dim_icd_reference` AS main
SET main.effective_end_date = CURRENT_DATETIME(), main.is_current = FALSE
WHERE EXISTS (
    SELECT 1
    FROM `{bq_stg_project}.{bq_stg_dataset}.dim_icd_reference` AS staging
    WHERE main.icd_code = staging.icd_code
        AND main.is_current
        AND (
            main.description != staging.description OR
            main.effective_date != staging.effective_date OR
            main.status != staging.status
        )
);

-- Step 2: Insert new records for changes
INSERT INTO `{bq_cur_project}.{bq_cur_dataset}.dim_icd_reference` 
(icd_code, description, effective_date, status,effective_start_date, effective_end_date, is_current)
SELECT 
    icd_code,
    description,
    effective_date,
    status,
    CURRENT_DATETIME() AS effective_start_date,
    effective_end_date,
    TRUE as is_current
FROM `{bq_stg_project}.{bq_stg_dataset}.dim_icd_reference` AS staging
WHERE NOT EXISTS (
    SELECT 1
    FROM `{bq_cur_project}.{bq_cur_dataset}.dim_icd_reference` AS main
    WHERE main.icd_code = staging.icd_code
        AND main.is_current
);
