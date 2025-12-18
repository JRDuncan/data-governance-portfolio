#!/bin/bash
echo "🔹 Starting Great Expectations Runner..."

# Activate environment
python3 run_ge_checks.py

echo "✅ Data Quality Checks Completed."
tail -f /dev/null

