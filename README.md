# Student Analytics

Interactive Power BI dashboard project built on the Open University Learning Analytics Dataset (OULAD). The repository focuses on data transformation and feature engineering first, then exports clean analytical tables that can be loaded into Power BI for dashboarding.

## Project Structure

- `data/`: raw OULAD CSV files
- `sql/`: SQL transformations that build student-level and cohort-level analytical tables
- `src/pipeline.py`: Python pipeline that loads CSVs into SQLite, runs SQL transformations, computes risk scores, and exports final CSVs
- `output/`: generated pipeline outputs (created when the pipeline runs)

## What The Pipeline Produces

Running the pipeline creates these CSV files in `output/`:

- `student_risk.csv`: student-level feature table with a derived early-alert risk score and risk tier
- `funnel_summary.csv`: registration-to-passing funnel metrics by module presentation
- `cohort_trends.csv`: weekly engagement trends by cohort and final result
- `assessment_summary.csv`: full-course assessment aggregates per student

These outputs are intended to be the main Power BI inputs.

## Requirements

- Python 3.11 or above recommended

## Run The Pipeline

From the project root:

```powershell
python src\pipeline.py
```

The script will:

1. Load the raw OULAD CSV files from `data/` into an in-memory SQLite database.
2. Run the SQL scripts in `sql/` in sequence.
3. Compute the student risk score in Python/pandas.
4. Export final analytical CSVs into `output/`.

## Using The Outputs In Power BI

After the pipeline finishes:

1. Open Power BI Desktop.
2. Import the generated CSV files from the `output/` folder.
3. Build visuals from the exported tables.