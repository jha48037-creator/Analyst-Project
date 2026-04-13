"""
PROJECT RUNNER — execute all steps in order
Run: python run_project.py
"""
import subprocess, sys, os

steps = [
    ("Step 1: Generate Data",  "01_data_generator.py"),
    ("Step 2: ETL Pipeline",   "02_etl_pipeline.py"),
    ("Step 3: ELT Pipeline",   "03_elt_pipeline.py"),
    ("Step 5: EDA Analysis",   "05_eda_analysis.py"),
    ("Step 6: KPIs",           "06_kpis.py"),
]

os.makedirs("logs", exist_ok=True)

for label, script in steps:
    print(f"\n{'='*55}")
    print(f"  {label}")
    print(f"{'='*55}")
    result = subprocess.run([sys.executable, script], capture_output=False)
    if result.returncode != 0:
        print(f"  ❌ {script} failed. Fix errors before continuing.")
        break
    print(f"  ✅ {script} completed")

print("\n\n  📁 OUTPUTS:")
print("    data/raw/            — 5 CSV source files")
print("    data/processed/      — SQLite databases (ETL + ELT)")
print("    output/eda/          — 9 EDA charts (.png)")
print("    logs/                — ETL pipeline logs")
print("\n  📄 SQL QUERIES: 04_sql_queries.sql  (run in DB Browser / DBeaver)")
print("  📊 POWER BI:    07_powerbi_guide.md  (step-by-step setup)")
