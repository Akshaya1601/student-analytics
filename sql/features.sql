-- =============================================================================
-- Step 2: Early features
-- =============================================================================
-- Computes early-semester engagement and assessment features per student.
-- "Early" is defined as the first 30 days of the module presentation,
-- which is the window where intervention is still practical.
--
-- VLE features (from studentVle):
--   - Total clicks in first 30 days
--   - Number of distinct active days
--   - Number of distinct resources accessed
--
-- Assessment features (from studentAssessment joined to assessments):
--   - Excludes banked assessments (carried over from prior attempts)
--   - Restricted to TMA and CMA types submitted within the first 30 days
--   - Average score, submission count, fail count, average days late
--
-- Also produces a full-course assessment summary used in the export.
-- =============================================================================

-- Early VLE engagement features -------------------------------------------------

CREATE TABLE IF NOT EXISTS early_vle_features AS

SELECT
    sv.id_student,
    sv.code_module,
    sv.code_presentation,
    SUM(sv.sum_click)        AS early_total_clicks,
    COUNT(DISTINCT sv.date)  AS early_active_days,
    COUNT(DISTINCT sv.id_site) AS early_resources_accessed
FROM studentVle sv
WHERE sv.date <= 30
GROUP BY
    sv.id_student,
    sv.code_module,
    sv.code_presentation;


-- Early assessment features (first 30 days, TMA and CMA only) -------------------

CREATE TABLE IF NOT EXISTS early_assess_features AS

SELECT
    sa.id_student,
    a.code_module,
    a.code_presentation,
    1                                                       AS has_early_assessment,
    AVG(sa.score)                                           AS early_avg_score,
    COUNT(*)                                                AS early_assessments_submitted,
    SUM(CASE WHEN sa.score < 40 THEN 1 ELSE 0 END)         AS early_fails,
    AVG(CAST(sa.date_submitted AS REAL) - a.date)          AS early_avg_days_late
FROM studentAssessment sa
JOIN assessments a
    ON sa.id_assessment = a.id_assessment
WHERE sa.is_banked        = 0
  AND a.assessment_type  IN ('TMA', 'CMA')
  AND sa.date_submitted  <= 30
GROUP BY
    sa.id_student,
    a.code_module,
    a.code_presentation;


-- Full-course assessment summary (all types, all dates) -------------------------

CREATE TABLE IF NOT EXISTS assessment_summary AS

SELECT
    sa.id_student,
    a.code_module,
    a.code_presentation,
    COUNT(*)                                                AS total_assessments_submitted,
    AVG(sa.score)                                           AS overall_avg_score,
    AVG(CAST(sa.date_submitted AS REAL) - a.date)          AS overall_avg_days_late,
    SUM(CASE WHEN sa.score < 40 THEN 1 ELSE 0 END)         AS total_failed_assessments,

    -- Weighted average score using assessment weight column.
    -- NULLIF prevents division by zero if all weights are NULL.
    SUM(sa.score * COALESCE(a.weight, 1.0)) /
        NULLIF(SUM(COALESCE(a.weight, 1.0)), 0)            AS weighted_avg_score

FROM studentAssessment sa
JOIN assessments a
    ON sa.id_assessment = a.id_assessment
WHERE sa.is_banked = 0
GROUP BY
    sa.id_student,
    a.code_module,
    a.code_presentation;
