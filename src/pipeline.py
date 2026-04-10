"""
OULAD Early Alert Risk Dashboard - Pipeline Orchestrator.

This pipeline loads raw OULAD CSVs into an in-memory SQLite database,
runs SQL transformation layers, computes an explainable early-alert risk score
in pandas, and exports dashboard-ready CSV files to `output/`.
"""

from pathlib import Path
import sqlite3
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
SQL_DIR = ROOT_DIR / "sql"
OUT_DIR = ROOT_DIR / "output"
OUT_DIR.mkdir(exist_ok=True)

SQL_FILES = [
    "load_and_join.sql",
    "features.sql",
    "funnel.sql",
    "cohort_trends.sql",
]

CSV_TABLES = {
    "courses": "courses.csv",
    "assessments": "assessments.csv",
    "vle": "vle.csv",
    "studentInfo": "studentInfo.csv",
    "studentRegistration": "studentRegistration.csv",
    "studentAssessment": "studentAssessment.csv",
    "studentVle": "studentVle.csv",
}

JOIN_KEYS = ["id_student", "code_module", "code_presentation"]
RISK_EXPORT_COLS = [
    "id_student", "code_module", "code_presentation",
    "gender", "region", "age_band", "age_rank",
    "highest_education", "education_rank",
    "imd_band", "imd_rank", "has_imd_info",
    "disability", "num_of_prev_attempts", "is_repeat_attempt",
    "studied_credits",
    "date_registration",
    "early_total_clicks", "early_active_days", "early_resources_accessed",
    "has_early_assessment", "early_assessments_submitted", "early_avg_score",
    "early_fails", "early_avg_days_late",
    "total_assessments_submitted", "overall_avg_score", "weighted_avg_score",
    "risk_vle", "risk_assess", "risk_timeliness", "risk_repeat", "risk_imd", "risk_reg",
    "risk_score_total", "risk_tier",
    "final_result", "did_not_complete",
]


def scale_to_risk(
    series: pd.Series,
    low_is_risky: bool,
    max_points: float,
    fill_strategy: str = "median",
) -> pd.Series:
    """Linearly rescale a numeric series into a risk contribution."""
    if fill_strategy == "median":
        filled = series.fillna(series.median())
    elif fill_strategy == "zero":
        filled = series.fillna(0)
    else:
        raise ValueError(f"Unsupported fill strategy: {fill_strategy}")

    col_min, col_max = filled.min(), filled.max()
    if col_max == col_min:
        return pd.Series(max_points / 2, index=series.index)

    normalised = (filled - col_min) / (col_max - col_min)
    if low_is_risky:
        normalised = 1 - normalised
    return (normalised * max_points).round(2)


def assign_risk_tiers(risk_scores: pd.Series) -> tuple[pd.Series, float, float]:
    """Split students into terciles for dashboard-friendly risk buckets."""
    p33 = risk_scores.quantile(0.33)
    p66 = risk_scores.quantile(0.66)

    def assign_tier(score: float) -> str:
        if score <= p33:
            return "Low"
        if score <= p66:
            return "Medium"
        return "High"

    return risk_scores.apply(assign_tier), p33, p66


def load_csv_tables(conn: sqlite3.Connection) -> None:
    print("Connecting to in-memory SQLite database...")
    print("Loading CSV files into database tables...")

    for table_name, filename in CSV_TABLES.items():
        filepath = DATA_DIR / filename
        df = pd.read_csv(filepath)
        df.columns = df.columns.str.strip()
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"  {table_name:<22} {len(df):>8,} rows")


def run_sql_scripts(conn: sqlite3.Connection) -> None:
    print("\nRunning SQL scripts...")
    for sql_file in SQL_FILES:
        sql = (SQL_DIR / sql_file).read_text()
        conn.executescript(sql)
        print(f"  Executed: {sql_file}")


def load_analytic_tables(conn: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    print("\nLoading SQL results into pandas...")
    tables = {
        "base": pd.read_sql("SELECT * FROM base_students", conn),
        "early_vle": pd.read_sql("SELECT * FROM early_vle_features", conn),
        "early_ass": pd.read_sql("SELECT * FROM early_assess_features", conn),
        "assessment_summary": pd.read_sql("SELECT * FROM assessment_summary", conn),
        "funnel": pd.read_sql("SELECT * FROM funnel_summary", conn),
        "trends": pd.read_sql("SELECT * FROM cohort_trends", conn),
    }

    print(f"  base_students rows        : {len(tables['base']):,}")
    print(f"  early_vle_features rows   : {len(tables['early_vle']):,}")
    print(f"  early_assess_features rows: {len(tables['early_ass']):,}")
    print(f"  assessment_summary rows   : {len(tables['assessment_summary']):,}")
    print(f"  funnel_summary rows       : {len(tables['funnel']):,}")
    print(f"  cohort_trends rows        : {len(tables['trends']):,}")

    return tables


def build_student_risk_dataset(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    print("\nMerging feature tables...")
    risk = tables["base"].copy()
    risk = risk.merge(tables["early_vle"], on=JOIN_KEYS, how="left")
    risk = risk.merge(tables["early_ass"], on=JOIN_KEYS, how="left")
    risk = risk.merge(tables["assessment_summary"], on=JOIN_KEYS, how="left")

    risk["has_imd_info"] = risk["imd_rank"].notna().astype(int)
    risk["has_early_assessment"] = risk["has_early_assessment"].fillna(0).astype(int)

    vle_fill_cols = ["early_total_clicks", "early_active_days", "early_resources_accessed"]
    risk[vle_fill_cols] = risk[vle_fill_cols].fillna(0)

    assessment_count_cols = ["early_assessments_submitted", "early_fails"]
    risk[assessment_count_cols] = risk[assessment_count_cols].fillna(0)

    return risk


def add_risk_scores(risk: pd.DataFrame) -> pd.DataFrame:
    """
    Build a 0-100 early-alert score with transparent component weights.

    Weighting rationale:
      Early VLE engagement   0-35 points
      Early assessment score 0-25 points
      Submission timeliness  0-15 points
      Repeat attempt         0-10 points
      Deprivation band       0-10 points
      Registration timing    0-05 points

    Students with no early assessment are assigned maximum assessment and
    timeliness risk because the absence of an early submission is itself an
    important intervention signal by day 30.
    """
    print("Computing risk scores...")

    risk["risk_vle"] = scale_to_risk(
        risk["early_total_clicks"],
        low_is_risky=True,
        max_points=35,
        fill_strategy="zero",
    )

    scored_assessment_risk = scale_to_risk(
        risk["early_avg_score"],
        low_is_risky=True,
        max_points=25,
    )
    risk["risk_assess"] = scored_assessment_risk.where(
        risk["has_early_assessment"] == 1,
        25.0,
    )

    scored_timeliness_risk = scale_to_risk(
        risk["early_avg_days_late"],
        low_is_risky=False,
        max_points=15,
    )
    risk["risk_timeliness"] = scored_timeliness_risk.where(
        risk["has_early_assessment"] == 1,
        15.0,
    )

    risk["risk_repeat"] = (risk["is_repeat_attempt"] * 10).astype(float)
    risk["risk_imd"] = scale_to_risk(
        risk["imd_rank"],
        low_is_risky=True,
        max_points=10,
    )
    risk["risk_reg"] = scale_to_risk(
        risk["date_registration"],
        low_is_risky=False,
        max_points=5,
    )

    component_cols = [
        "risk_vle",
        "risk_assess",
        "risk_timeliness",
        "risk_repeat",
        "risk_imd",
        "risk_reg",
    ]
    risk["risk_score_total"] = risk[component_cols].sum(axis=1).clip(0, 100).round(1)

    risk["risk_tier"], p33, p66 = assign_risk_tiers(risk["risk_score_total"])
    print(f"  Tercile thresholds: p33={p33:.1f}, p66={p66:.1f}")
    print(f"  Risk tier distribution:\n{risk['risk_tier'].value_counts()}")

    return risk


def export_outputs(
    risk: pd.DataFrame,
    funnel: pd.DataFrame,
    trends: pd.DataFrame,
    assessment_summary: pd.DataFrame,
) -> None:
    print("\nExporting output files...")
    risk[RISK_EXPORT_COLS].to_csv(OUT_DIR / "student_risk.csv", index=False)
    funnel.to_csv(OUT_DIR / "funnel_summary.csv", index=False)
    trends.to_csv(OUT_DIR / "cohort_trends.csv", index=False)
    assessment_summary.to_csv(OUT_DIR / "assessment_summary.csv", index=False)

    print("  student_risk.csv")
    print("  funnel_summary.csv")
    print("  cohort_trends.csv")
    print("  assessment_summary.csv")


def run_validations(risk: pd.DataFrame, funnel: pd.DataFrame) -> None:
    print("\nSanity checks:")
    print(f"  student_risk shape       : {risk[RISK_EXPORT_COLS].shape}")
    print(f"  risk_score_total stats:\n{risk['risk_score_total'].describe().round(2)}")

    key_cols = ["early_total_clicks", "early_avg_score", "imd_rank", "risk_score_total"]
    null_counts = risk[key_cols].isnull().sum()
    print(f"\n  Null counts in key columns:\n{null_counts}")

    print("\nBusiness-facing validation checks:")
    print(f"  Students with zero early clicks      : {(risk['early_total_clicks'] == 0).sum():,}")
    print(f"  Students with no early assessment    : {(risk['has_early_assessment'] == 0).sum():,}")
    print(f"  Students missing IMD band            : {(risk['has_imd_info'] == 0).sum():,}")

    outcome_by_tier = (
        risk.groupby("risk_tier", observed=False)["did_not_complete"]
        .agg(["count", "mean"])
        .rename(columns={"count": "students", "mean": "did_not_complete_rate"})
        .round({"did_not_complete_rate": 3})
    )
    print(f"\n  Did-not-complete rate by risk tier:\n{outcome_by_tier}")

    funnel_order_violations = (
        (funnel["n_active"] > funnel["n_registered"]) |
        (funnel["n_submitted"] > funnel["n_active"]) |
        (funnel["n_passed"] > funnel["n_submitted"])
    ).sum()
    print(f"\n  Funnel stage order violations        : {funnel_order_violations}")


def main() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        load_csv_tables(conn)
        run_sql_scripts(conn)
        tables = load_analytic_tables(conn)
        risk = build_student_risk_dataset(tables)
        risk = add_risk_scores(risk)
        export_outputs(
            risk=risk,
            funnel=tables["funnel"],
            trends=tables["trends"],
            assessment_summary=tables["assessment_summary"],
        )
        run_validations(risk, tables["funnel"])
    finally:
        conn.close()
        print("\nDatabase connection closed. Pipeline complete.")


if __name__ == "__main__":
    main()
