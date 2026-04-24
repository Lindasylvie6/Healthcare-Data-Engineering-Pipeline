-- =============================================
-- SILVER: denials
-- Source: bronze.denials_raw
-- Rows expected: 5,998
-- Key transforms: date casting, amount casting,
--                 appeal_filed to BIT,
--                 days_to_resolution calculated
-- =============================================
CREATE OR ALTER PROCEDURE silver.load_denials
AS
BEGIN
    -- Clear silver table before loading
    TRUNCATE TABLE silver.denials;

    -- Insert cleaned, typed data from bronze
    INSERT INTO silver.denials (
        claim_id,
        denial_id,
        denial_reason_code,
        denial_reason_description,
        denied_amount,
        denial_date,
        appeal_filed,
        appeal_status,
        appeal_resolution_date,
        final_outcome,
        days_to_resolution,
        silver_load_dt
    )
    SELECT
        claim_id,
        denial_id,
        denial_reason_code,
        denial_reason_description,
        -- Cast denied amount to decimal
        TRY_CAST(denied_amount AS DECIMAL(18,2))        AS denied_amount,
        -- Convert DD-MM-YYYY to proper DATE
        TRY_CONVERT(DATE, denial_date, 105)             AS denial_date,
        -- Convert Yes/No to BIT
        CASE
            WHEN UPPER(appeal_filed) = 'YES' THEN 1
            WHEN UPPER(appeal_filed) = 'NO'  THEN 0
            ELSE NULL
        END                                             AS appeal_filed,
        appeal_status,
        -- Convert resolution date to proper DATE
        TRY_CONVERT(DATE, appeal_resolution_date, 105)  AS appeal_resolution_date,
        final_outcome,
        -- Calculate days from denial to resolution
        CASE
            WHEN TRY_CONVERT(DATE, appeal_resolution_date, 105) IS NOT NULL
            AND  TRY_CONVERT(DATE, denial_date, 105) IS NOT NULL
            THEN DATEDIFF(day,
                TRY_CONVERT(DATE, denial_date, 105),
                TRY_CONVERT(DATE, appeal_resolution_date, 105))
            ELSE NULL
        END                                             AS days_to_resolution,
        -- Timestamp when loaded to silver
        GETUTCDATE()                                    AS silver_load_dt
    FROM bronze.denials_raw
    -- Only load records with a valid denial_id
    WHERE denial_id IS NOT NULL;

END;
GO

-- Execute the stored procedure
EXEC silver.load_denials;

-- =============================================
-- SILVER: claims
-- Source: bronze.claims_raw
-- Rows expected: 70,000
-- Key transforms: date casting, amount casting,
--                 is_denied flag derived,
--                 missing claim_id handled
-- =============================================
CREATE OR ALTER PROCEDURE silver.load_claims
AS
BEGIN
    TRUNCATE TABLE silver.claims;

    INSERT INTO silver.claims (
        billing_id,
        patient_id,
        encounter_id,
        claim_id,
        insurance_provider,
        payment_method,
        claim_billing_date,
        billed_amount,
        paid_amount,
        claim_status,
        denial_reason,
        is_denied,
        silver_load_dt
    )
    SELECT
        billing_id,
        patient_id,
        encounter_id,
        -- Keep all rows, assign surrogate ID if missing
        COALESCE(claim_id, 'NO-CLAIM-' + billing_id) AS claim_id,
        insurance_provider,
        payment_method,
        TRY_CONVERT(DATE, claim_billing_date, 105)    AS claim_billing_date,
        TRY_CAST(billed_amount AS DECIMAL(18,2))      AS billed_amount,
        TRY_CAST(paid_amount AS DECIMAL(18,2))        AS paid_amount,
        claim_status,
        denial_reason,
        CASE
            WHEN UPPER(claim_status) = 'DENIED' THEN 1
            ELSE 0
        END                                           AS is_denied,
        GETUTCDATE()                                  AS silver_load_dt
    FROM bronze.claims_raw
    WHERE billing_id IS NOT NULL;

END;
GO
  
-- Execute the stored procedure
EXEC silver.load_claims;

-- =============================================
-- SILVER: denials
-- Source: bronze.denials_raw
-- Rows expected: 5,998
-- Key transforms: date casting, amount casting,
--                 appeal_filed to BIT,
--                 days_to_resolution calculated
-- =============================================
CREATE OR ALTER PROCEDURE silver.load_denials
AS
BEGIN
    -- Clear silver table before loading
    TRUNCATE TABLE silver.denials;

    -- Insert cleaned, typed data from bronze
    INSERT INTO silver.denials (
        claim_id,
        denial_id,
        denial_reason_code,
        denial_reason_description,
        denied_amount,
        denial_date,
        appeal_filed,
        appeal_status,
        appeal_resolution_date,
        final_outcome,
        days_to_resolution,
        silver_load_dt
    )
    SELECT
        claim_id,
        denial_id,
        denial_reason_code,
        denial_reason_description,
        -- Cast denied amount to decimal
        TRY_CAST(denied_amount AS DECIMAL(18,2))        AS denied_amount,
        -- Convert DD-MM-YYYY to proper DATE
        TRY_CONVERT(DATE, denial_date, 105)             AS denial_date,
        -- Convert Yes/No to BIT
        CASE
            WHEN UPPER(appeal_filed) = 'YES' THEN 1
            WHEN UPPER(appeal_filed) = 'NO'  THEN 0
            ELSE NULL
        END                                             AS appeal_filed,
        appeal_status,
        -- Convert resolution date to proper DATE
        TRY_CONVERT(DATE, appeal_resolution_date, 105)  AS appeal_resolution_date,
        final_outcome,
        -- Calculate days from denial to resolution
        CASE
            WHEN TRY_CONVERT(DATE, appeal_resolution_date, 105) IS NOT NULL
            AND  TRY_CONVERT(DATE, denial_date, 105) IS NOT NULL
            THEN DATEDIFF(day,
                TRY_CONVERT(DATE, denial_date, 105),
                TRY_CONVERT(DATE, appeal_resolution_date, 105))
            ELSE NULL
        END                                             AS days_to_resolution,
        -- Timestamp when loaded to silver
        GETUTCDATE()                                    AS silver_load_dt
    FROM bronze.denials_raw
    -- Only load records with a valid denial_id
    WHERE denial_id IS NOT NULL;

END;
GO

-- Execute the stored procedure
EXEC silver.load_denials;

-- =============================================
-- SILVER: diagnoses
-- Source: bronze.diagnoses_raw
-- Rows expected: 70,000
-- Key transforms: primary_flag to BIT,
--                 chronic_flag to BIT
-- =============================================
CREATE OR ALTER PROCEDURE silver.load_diagnoses
AS
BEGIN
    -- Clear silver table before loading
    TRUNCATE TABLE silver.diagnoses;

    -- Insert cleaned, typed data from bronze
    INSERT INTO silver.diagnoses (
        diagnosis_id,
        encounter_id,
        diagnosis_code,
        diagnosis_description,
        primary_flag,
        chronic_flag,
        silver_load_dt
    )
    SELECT
        diagnosis_id,
        encounter_id,
        -- Uppercase for consistency
        UPPER(diagnosis_code)                    AS diagnosis_code,
        diagnosis_description,
        -- Convert TRUE/FALSE string to BIT
        CASE
            WHEN UPPER(primary_flag) = 'TRUE'  THEN 1
            WHEN UPPER(primary_flag) = 'FALSE' THEN 0
            ELSE NULL
        END                                      AS primary_flag,
        -- Convert TRUE/FALSE string to BIT
        CASE
            WHEN UPPER(chronic_flag) = 'TRUE'  THEN 1
            WHEN UPPER(chronic_flag) = 'FALSE' THEN 0
            ELSE NULL
        END                                      AS chronic_flag,
        -- Timestamp when loaded to silver
        GETUTCDATE()                             AS silver_load_dt
    FROM bronze.diagnoses_raw
    -- Only load records with valid diagnosis_id
    WHERE diagnosis_id IS NOT NULL;

END;
GO

-- Execute the stored procedure
EXEC silver.load_diagnoses;

-- Verify row count
SELECT COUNT(*) AS silver_diagnoses FROM silver.diagnoses;

-- Check chronic vs non-chronic breakdown
-- This is a key population health metric
SELECT
    chronic_flag,
    primary_flag,
    COUNT(*)                    AS count,
    COUNT(DISTINCT encounter_id) AS unique_encounters
FROM silver.diagnoses
GROUP BY chronic_flag, primary_flag
ORDER BY count DESC;

-- =============================================
-- SILVER: procedures
-- Source: bronze.procedures_raw
-- Rows expected: 126,021
-- Key transforms: date casting, cost casting
-- =============================================
CREATE OR ALTER PROCEDURE silver.load_procedures
AS
BEGIN
    -- Clear silver table before loading
    TRUNCATE TABLE silver.procedures;

    -- Insert cleaned, typed data from bronze
    INSERT INTO silver.procedures (
        procedure_id,
        encounter_id,
        procedure_code,
        procedure_description,
        procedure_date,
        provider_id,
        procedure_cost,
        silver_load_dt
    )
    SELECT
        procedure_id,
        encounter_id,
        -- Uppercase for consistency
        UPPER(procedure_code)                       AS procedure_code,
        procedure_description,
        -- Convert DD-MM-YYYY to proper DATE
        TRY_CONVERT(DATE, procedure_date, 105)      AS procedure_date,
        provider_id,
        -- Cast cost to decimal
        TRY_CAST(procedure_cost AS DECIMAL(18,2))   AS procedure_cost,
        -- Timestamp when loaded to silver
        GETUTCDATE()                                AS silver_load_dt
    FROM bronze.procedures_raw
    -- Only load records with valid procedure_id
    WHERE procedure_id IS NOT NULL;

END;
GO
  -- Execute the stored procedure
    EXEC silver.load_procedures;

-- =============================================
-- SILVER: lab_tests
-- Source: bronze.lab_tests_raw
-- Rows expected: 54,537
-- Key transforms: date casting,
--                 is_abnormal flag derived
-- =============================================
CREATE OR ALTER PROCEDURE silver.load_lab_tests
AS
BEGIN
    -- Clear silver table before loading
    TRUNCATE TABLE silver.lab_tests;

    -- Insert cleaned, typed data from bronze
    INSERT INTO silver.lab_tests (
        lab_id,
        encounter_id,
        test_name,
        test_code,
        specimen_type,
        test_result,
        units,
        normal_range,
        test_date,
        status,
        is_abnormal,
        silver_load_dt
    )
    SELECT
        lab_id,
        encounter_id,
        test_name,
        test_code,
        specimen_type,
        test_result,
        units,
        normal_range,
        -- Convert DD-MM-YYYY to proper DATE
        TRY_CONVERT(DATE, test_date, 105)           AS test_date,
        status,
        -- Derive abnormal flag from result and status
        CASE
            WHEN UPPER(test_result) IN ('ABNORMAL','POSITIVE','HIGH','LOW') THEN 1
            WHEN UPPER(status) = 'ABNORMAL' THEN 1
            ELSE 0
        END                                         AS is_abnormal,
        -- Timestamp when loaded to silver
        GETUTCDATE()                                AS silver_load_dt
    FROM bronze.lab_tests_raw
    -- Only load records with valid lab_id
    WHERE lab_id IS NOT NULL;

END;
GO
  -- Execute the stored procedure
    EXEC silver.load_lab_tests;

-- =============================================
-- SILVER: medications
-- Source: bronze.medications_raw
-- Rows expected: 94,498
-- Key transforms: date casting, cost casting,
--                 route standardized
-- =============================================
CREATE OR ALTER PROCEDURE silver.load_medications
AS
BEGIN
    -- Clear silver table before loading
    TRUNCATE TABLE silver.medications;

    -- Insert cleaned, typed data from bronze
    INSERT INTO silver.medications (
        medication_id,
        encounter_id,
        drug_name,
        dosage,
        route,
        frequency,
        duration,
        prescribed_date,
        prescriber_id,
        cost,
        silver_load_dt
    )
    SELECT
        medication_id,
        encounter_id,
        -- Proper case for drug names
        drug_name,
        dosage,
        -- Standardize route to uppercase
        UPPER(route)                                AS route,
        frequency,
        duration,
        -- Convert DD-MM-YYYY to proper DATE
        TRY_CONVERT(DATE, prescribed_date, 105)     AS prescribed_date,
        prescriber_id,
        -- Cast cost to decimal
        TRY_CAST(cost AS DECIMAL(18,2))             AS cost,
        -- Timestamp when loaded to silver
        GETUTCDATE()                                AS silver_load_dt
    FROM bronze.medications_raw
    -- Only load records with valid medication_id
    WHERE medication_id IS NOT NULL;

END;
GO
  -- Execute the stored procedure
EXEC silver.load_medications;

-- =============================================
-- SILVER: providers
-- Source: bronze.providers_raw
-- Rows expected: 1,491
-- Key transforms: inhouse to BIT,
--                 years_experience to INT
-- =============================================
CREATE OR ALTER PROCEDURE silver.load_providers
AS
BEGIN
    -- Clear silver table before loading
    TRUNCATE TABLE silver.providers;

    -- Insert cleaned, typed data from bronze
    INSERT INTO silver.providers (
        provider_id,
        name,
        department,
        specialty,
        npi,
        inhouse,
        location,
        years_experience,
        contact_info,
        email,
        silver_load_dt
    )
    SELECT
        provider_id,
        name,
        department,
        specialty,
        npi,
        -- Convert Yes/No to BIT
        CASE
            WHEN UPPER(inhouse) = 'YES' THEN 1
            WHEN UPPER(inhouse) = 'NO'  THEN 0
            ELSE NULL
        END                                         AS inhouse,
        location,
        -- Cast years experience to INT
        TRY_CAST(years_experience AS INT)           AS years_experience,
        contact_info,
        email,
        -- Timestamp when loaded to silver
        GETUTCDATE()                                AS silver_load_dt
    FROM bronze.providers_raw
    -- Only load records with valid provider_id
    WHERE provider_id IS NOT NULL;

END;
GO
  -- Execute the stored procedure
EXEC silver.load_providers;

-- =============================================
-- SILVER: encounters
-- Source: bronze.encounters_raw
-- Rows expected: 70,000
-- Key transforms: date casting, LOS calculated,
--                 readmitted_flag to BIT
-- =============================================
CREATE OR ALTER PROCEDURE silver.load_encounters
AS
BEGIN
    -- Clear silver table before loading
    TRUNCATE TABLE silver.encounters;

    -- Insert cleaned, typed data from bronze
    INSERT INTO silver.encounters (
        encounter_id,
        patient_id,
        provider_id,
        visit_date,
        visit_type,
        department,
        reason_for_visit,
        diagnosis_code,
        admission_type,
        discharge_date,
        length_of_stay,
        status,
        readmitted_flag,
        silver_load_dt
    )
    SELECT
        encounter_id,
        patient_id,
        provider_id,
        -- Convert DD-MM-YYYY to proper DATE
        TRY_CONVERT(DATE, visit_date, 105)          AS visit_date,
        visit_type,
        department,
        reason_for_visit,
        diagnosis_code,
        admission_type,
        -- Convert discharge date to proper DATE
        TRY_CONVERT(DATE, discharge_date, 105)      AS discharge_date,
        -- Calculate Length of Stay in days
        CASE
            WHEN TRY_CONVERT(DATE, discharge_date, 105) IS NOT NULL
            AND  TRY_CONVERT(DATE, visit_date, 105) IS NOT NULL
            THEN DATEDIFF(day,
                TRY_CONVERT(DATE, visit_date, 105),
                TRY_CONVERT(DATE, discharge_date, 105))
            ELSE NULL
        END                                         AS length_of_stay,
        status,
        -- Convert Yes/No to BIT
        CASE
            WHEN UPPER(readmitted_flag) = 'YES' THEN 1
            WHEN UPPER(readmitted_flag) = 'NO'  THEN 0
            ELSE NULL
        END                                         AS readmitted_flag,
        -- Timestamp when loaded to silver
        GETUTCDATE()                                AS silver_load_dt
    FROM bronze.encounters_raw
    -- Only load records with valid encounter_id
    WHERE encounter_id IS NOT NULL;

END;
GO

-- Execute the stored procedure
EXEC silver.load_encounters;


-- =============================================
-- VERIFY ALL SILVER TABLES
-- =============================================
SELECT 'patients'   AS table_name, COUNT(*) AS rows FROM silver.patients   UNION ALL
SELECT 'encounters',               COUNT(*)          FROM silver.encounters UNION ALL
SELECT 'claims',                   COUNT(*)          FROM silver.claims     UNION ALL
SELECT 'denials',                  COUNT(*)          FROM silver.denials    UNION ALL
SELECT 'diagnoses',                COUNT(*)          FROM silver.diagnoses  UNION ALL
SELECT 'procedures',               COUNT(*)          FROM silver.procedures UNION ALL
SELECT 'lab_tests',                COUNT(*)          FROM silver.lab_tests  UNION ALL
SELECT 'medications',              COUNT(*)          FROM silver.medications UNION ALL
SELECT 'providers',                COUNT(*)          FROM silver.providers
ORDER BY table_name;



