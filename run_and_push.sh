#!/bin/bash
# Run forecast and push to GitHub
# Usage: ./run_and_push.sh

set -e

cd "$(dirname "$0")"

echo "=== Running forecast ==="
./venv/Scripts/python.exe -m duck_sun.scheduler

echo ""
echo "=== Committing outputs ==="
git add outputs/ reports/

# Check if there are changes to commit
if git diff --cached --quiet; then
    echo "No new files to commit"
else
    DATE=$(date +%Y-%m-%d)
    git commit -m "Forecast: $DATE"

    echo ""
    echo "=== Pushing to GitHub ==="
    git push
fi

echo ""
echo "=== Done ==="
