-- =============================================================================
-- Step 4: Cohort trends
-- =============================================================================
-- Produces weekly VLE engagement totals broken down by cohort
-- (module + presentation) and student outcome group.
--
-- Week number is derived by dividing the interaction date by 7 and
-- taking the ceiling, so day 1-7 = week 1, day 8-14 = week 2, etc.
-- Day 0 (the module start day) is assigned to week 0.
--
-- This table drives the trend line / area chart on the funnel tab of
-- the Power BI dashboard, showing how engagement patterns diverge
-- between students who eventually pass versus those who withdraw or fail.
-- =============================================================================

CREATE TABLE IF NOT EXISTS cohort_trends AS

SELECT
    sv.code_module,
    sv.code_presentation,

    -- Week number: day 0 stays as week 0; days 1+ map to rounded-up 7-day buckets.
    -- SQLite in Python does not expose CEIL by default, so use integer arithmetic.
    CASE
        WHEN sv.date = 0 THEN 0
        ELSE CAST((sv.date + 6) / 7 AS INTEGER)
    END AS week_number,

    si.final_result,

    COUNT(DISTINCT sv.id_student) AS active_students,
    SUM(sv.sum_click)             AS total_clicks,
    AVG(sv.sum_click)             AS avg_clicks_per_student

FROM studentVle sv
JOIN studentInfo si
    ON  sv.id_student        = si.id_student
    AND sv.code_module       = si.code_module
    AND sv.code_presentation = si.code_presentation

-- Restrict final_result to the four canonical outcomes so that any
-- data quality rows do not create extra series in the visual
WHERE si.final_result IN ('Pass', 'Distinction', 'Fail', 'Withdrawn')

GROUP BY
    sv.code_module,
    sv.code_presentation,
    week_number,
    si.final_result;
