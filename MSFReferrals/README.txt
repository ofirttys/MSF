
# 1. Convert to SQLite
python Convert-CSV-To-SQLite.py referral-status-20260221.csv DB\referrals.db


# 2. Export and compare
python Convert-SQLite-To-CSV.py DB\referrals.db exported.csv
python Convert-SQLite-To-CSV.py DB\referrals.db exported.csv --compare referral-status-20260221.csv

# 3. Manual spot check (optional)
# Open both CSVs in Excel and compare a few rows side-by-side