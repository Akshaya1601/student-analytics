-- =============================================================================
-- Step 1: Load and join
-- =============================================================================
-- Builds the base student table by joining studentInfo with studentRegistration.
-- Handles:
--   - Normalising imd_band to a consistent "XX-XX%" format
--   - Mapping imd_band, age_band, and highest_education to ordinal ranks
--   - Flagging repeat attempts
--   - Classifying final_result into four canonical values
--   - Deriving a binary did_not_complete outcome label
--   - Flagging early withdrawals (within first 30 days)
-- =============================================================================

CREATE TABLE IF NOT EXISTS base_students AS

WITH imd_normalised AS (
    SELECT
        si.code_module,
        si.code_presentation,
        si.id_student,
        si.gender,
        si.region,
        si.highest_education,
        si.age_band,
        si.num_of_prev_attempts,
        si.studied_credits,
        si.disability,

        -- Normalise imd_band: strip whitespace and ensure trailing %
        CASE
            WHEN TRIM(si.imd_band) IN ('0-10',  '10-20', '20-30', '30-40', '40-50',
                                        '50-60', '60-70', '70-80', '80-90', '90-100')
                THEN TRIM(si.imd_band) || '%'
            ELSE TRIM(si.imd_band)
        END AS imd_band,

        -- Canonical final_result
        CASE
            WHEN si.final_result IN ('Pass', 'Fail', 'Withdrawn', 'Distinction')
                THEN si.final_result
            ELSE 'Unknown'
        END AS final_result

    FROM studentInfo si
),

ranked AS (
    SELECT
        *,

        -- IMD ordinal rank (1 = most deprived, 10 = least deprived)
        CASE imd_band
            WHEN '0-10%'   THEN 1
            WHEN '10-20%'  THEN 2
            WHEN '20-30%'  THEN 3
            WHEN '30-40%'  THEN 4
            WHEN '40-50%'  THEN 5
            WHEN '50-60%'  THEN 6
            WHEN '60-70%'  THEN 7
            WHEN '70-80%'  THEN 8
            WHEN '80-90%'  THEN 9
            WHEN '90-100%' THEN 10
            ELSE NULL
        END AS imd_rank,

        -- Age ordinal rank
        CASE age_band
            WHEN '0-35'  THEN 1
            WHEN '35-55' THEN 2
            WHEN '55<='  THEN 3
            ELSE NULL
        END AS age_rank,

        -- Education ordinal rank
        CASE highest_education
            WHEN 'No Formal quals'              THEN 1
            WHEN 'Lower Than A Level'           THEN 2
            WHEN 'A Level or Equivalent'        THEN 3
            WHEN 'HE Qualification'             THEN 4
            WHEN 'Post Graduate Qualification'  THEN 5
            ELSE NULL
        END AS education_rank,

        -- Repeat attempt flag
        CASE WHEN num_of_prev_attempts > 0 THEN 1 ELSE 0 END AS is_repeat_attempt,

        -- Binary outcome: 1 = did not complete (Withdrawn or Fail)
        CASE
            WHEN final_result IN ('Withdrawn', 'Fail') THEN 1
            ELSE 0
        END AS did_not_complete

    FROM imd_normalised
)

SELECT
    r.*,
    sr.date_registration,

    -- Keep -999 sentinel for students who never unregistered (completed)
    -- so the visualizations can filter on this without confusing NULL with missing data
    COALESCE(sr.date_unregistration, -999) AS date_unregistration,

    -- Early withdrawal: unregistered within the first 30 days
    CASE
        WHEN sr.date_unregistration IS NOT NULL
         AND sr.date_unregistration >= 0
         AND sr.date_unregistration <= 30
        THEN 1
        ELSE 0
    END AS early_withdrawal

FROM ranked r
LEFT JOIN studentRegistration sr
    ON  r.id_student          = sr.id_student
    AND r.code_module         = sr.code_module
    AND r.code_presentation   = sr.code_presentation;