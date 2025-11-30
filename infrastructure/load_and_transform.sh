#!/bin/bash
# Load CSV data and run dbt models
# This script loads CSV data into PostgreSQL and runs dbt transformations

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

echo "=========================================="
echo "Load Data and Run dbt Models"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Activate virtual environment
if [ ! -d "venv" ]; then
    echo -e "${RED}✗ Virtual environment not found. Run setup_infra.sh first.${NC}"
    exit 1
fi
source venv/bin/activate

# Step 1: Load CSV Data
echo -e "${GREEN}Step 1: Loading CSV data into PostgreSQL...${NC}"
if [ ! -d "csv_data" ] || [ -z "$(ls -A csv_data/*.csv 2>/dev/null)" ]; then
    echo -e "${YELLOW}⚠ No CSV files found in csv_data/ directory${NC}"
    echo "Skipping CSV data load..."
else
    python scripts/load_csv_data.py
    echo "✓ CSV data loaded"
fi
echo ""

# Step 2: Run dbt Models
echo -e "${GREEN}Step 2: Running dbt models...${NC}"
cd dbt
dbt run
echo "✓ dbt models created"
cd ..
echo ""

# Step 3: Run dbt Tests
echo -e "${GREEN}Step 3: Running dbt tests...${NC}"
cd dbt
dbt test
echo "✓ dbt tests completed"
cd ..

echo ""
echo "=========================================="
echo -e "${GREEN}✓ Data Loading and Transformation Complete!${NC}"
echo "=========================================="
echo ""

