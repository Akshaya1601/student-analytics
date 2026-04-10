-- =============================================================================
-- Step 3: Funnel
-- =============================================================================
-- Builds the engagement-to-outcome funnel summary per module-presentation.
--
-- Funnel stages (sequential):
--   1. Registered   - appears in studentRegistration
--   2. Active        - had at least 1 VLE click within the first 30 days
--   3. Submitted     - submitted at least 1 non-banked assessment within first 30 days
--   4. Passed        - final_result is Pass or Distinction
--
-- Also derives drop-off counts between each stage so Power BI can render
-- a funnel or waterfall chart directly without further transformation.
-- =============================================================================

CREATE TABLE IF NOT EXISTS funnel_summary AS

WITH registered AS (
    SELECT
        code_module,
        code_presentation,
        id_student,
        1 AS is_registered
    FROM studentRegistration
),

active AS (
    SELECT DISTINCT
        code_module,
        code_presentation,
        id_student,
        1 AS is_active
    FROM studentVle
    WHERE date <= 30
),

submitted AS (
    SELECT DISTINCT
        a.code_module,
        a.code_presentation,
        sa.id_student,
        1 AS is_submitted
    FROM studentAssessment sa
    JOIN assessments a
        ON sa.id_assessment = a.id_assessment
    WHERE sa.is_banked = 0
      AND sa.date_submitted <= 30
),

passed AS (
    SELECT
        code_module,
        code_presentation,
        id_student,
        1 AS is_passed
    FROM studentInfo
    WHERE final_result IN ('Pass', 'Distinction')
),

student_funnel AS (
    -- Enforce sequential stage attainment so downstream counts can never
    -- exceed upstream counts in the funnel visual.
    SELECT
        r.code_module,
        r.code_presentation,
        r.id_student,
        r.is_registered,
        COALESCE(ac.is_active, 0) AS is_active,
        CASE
            WHEN COALESCE(ac.is_active, 0) = 1
             AND COALESCE(su.is_submitted, 0) = 1
            THEN 1 ELSE 0
        END AS is_submitted,
        CASE
            WHEN COALESCE(ac.is_active, 0) = 1
             AND COALESCE(su.is_submitted, 0) = 1
             AND COALESCE(pa.is_passed, 0) = 1
            THEN 1 ELSE 0
        END AS is_passed
    FROM registered r
    LEFT JOIN active ac
        ON  r.id_student        = ac.id_student
        AND r.code_module       = ac.code_module
        AND r.code_presentation = ac.code_presentation
    LEFT JOIN submitted su
        ON  r.id_student        = su.id_student
        AND r.code_module       = su.code_module
        AND r.code_presentation = su.code_presentation
    LEFT JOIN passed pa
        ON  r.id_student        = pa.id_student
        AND r.code_module       = pa.code_module
        AND r.code_presentation = pa.code_presentation
)

SELECT
    code_module,
    code_presentation,
    SUM(is_registered)  AS n_registered,
    SUM(is_active)      AS n_active,
    SUM(is_submitted)   AS n_submitted,
    SUM(is_passed)      AS n_passed,

    -- Drop-off at each transition
    SUM(is_registered) - SUM(is_active)     AS drop_before_active,
    SUM(is_active)     - SUM(is_submitted)  AS drop_before_submitted,
    SUM(is_submitted)  - SUM(is_passed)     AS drop_before_pass,

    -- Conversion rates (rounded to 2 decimal places)
    ROUND(100.0 * SUM(is_active)    / NULLIF(SUM(is_registered), 0), 2) AS pct_active,
    ROUND(100.0 * SUM(is_submitted) / NULLIF(SUM(is_active),     0), 2) AS pct_submitted_of_active,
    ROUND(100.0 * SUM(is_passed)    / NULLIF(SUM(is_submitted),  0), 2) AS pct_passed_of_submitted,
    ROUND(100.0 * SUM(is_passed)    / NULLIF(SUM(is_registered), 0), 2) AS overall_pass_rate

FROM student_funnel
GROUP BY
    code_module,
    code_presentation;
