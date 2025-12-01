#!/bin/bash

# Initial Infrastructure Setup Script
# This script sets up the complete infrastructure for the data pipeline:
# - Creates Python virtual environment
# - Installs Python dependencies
# - Creates PostgreSQL database and schema
# - Configures dbt profiles
# - Installs dbt packages

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

echo "=========================================="
echo "Infrastructure Setup"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Step 1: Create Python virtual environment
echo -e "${BLUE}Step 1: Creating Python virtual environment...${NC}"
if [ -d "venv" ]; then
    echo -e "${YELLOW}⚠ Virtual environment already exists. Skipping...${NC}"
else
    python3 -m venv venv
    echo -e "${GREEN}✓ Virtual environment created${NC}"
fi
echo ""

# Step 2: Install Python dependencies
echo -e "${BLUE}Step 2: Installing Python dependencies...${NC}"
source venv/bin/activate
pip install --upgrade pip

# Install Airflow with constraints file to avoid build issues
echo "Installing Airflow with constraints (prevents build errors)..."
AIRFLOW_LINE=$(grep apache-airflow requirements.txt | head -1)

# Extract version number - handle >= format (e.g., "apache-airflow>=3.0.6" -> "3.0.6")
if echo "$AIRFLOW_LINE" | grep -q ">="; then
    AIRFLOW_VERSION=$(echo "$AIRFLOW_LINE" | sed 's/.*>=//' | tr -d ' ')
elif echo "$AIRFLOW_LINE" | grep -q "=="; then
    AIRFLOW_VERSION=$(echo "$AIRFLOW_LINE" | sed 's/.*==//' | tr -d ' ')
else
    # Default to a known working version
    AIRFLOW_VERSION="3.0.6"
fi

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.9.txt"

echo "Detected Airflow version: ${AIRFLOW_VERSION}"
echo "Using constraints from: $CONSTRAINT_URL"

# Install Airflow and requests with constraints first
if curl -s -f "$CONSTRAINT_URL" > /dev/null 2>&1; then
    pip install apache-airflow requests --constraint "$CONSTRAINT_URL"
    echo -e "${GREEN}✓ Airflow installed${NC}"
else
    echo -e "${YELLOW}⚠ Constraints file not found, installing Airflow without constraints...${NC}"
    pip install apache-airflow requests
fi

# Install dbt and psycopg2-binary separately (without constraints to avoid build issues)
echo "Installing dbt and PostgreSQL packages..."
pip install dbt-postgres psycopg2-binary
echo -e "${GREEN}✓ dbt packages installed${NC}"

echo -e "${GREEN}✓ Python dependencies installed${NC}"
echo ""

# Step 3: Create PostgreSQL database and schema
echo -e "${BLUE}Step 3: Setting up PostgreSQL database...${NC}"

# Try to find PostgreSQL in common locations if not in PATH
PSQL_PATH=""
if command -v psql > /dev/null 2>&1; then
    PSQL_PATH=$(command -v psql)
elif [ -f /opt/homebrew/opt/postgresql@15/bin/psql ]; then
    PSQL_PATH="/opt/homebrew/opt/postgresql@15/bin/psql"
    export PATH="/opt/homebrew/opt/postgresql@15/bin:$PATH"
elif [ -f /usr/local/opt/postgresql@15/bin/psql ]; then
    PSQL_PATH="/usr/local/opt/postgresql@15/bin/psql"
    export PATH="/usr/local/opt/postgresql@15/bin:$PATH"
elif [ -f /opt/homebrew/opt/postgresql@16/bin/psql ]; then
    PSQL_PATH="/opt/homebrew/opt/postgresql@16/bin/psql"
    export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"
elif [ -f /usr/local/opt/postgresql@16/bin/psql ]; then
    PSQL_PATH="/usr/local/opt/postgresql@16/bin/psql"
    export PATH="/usr/local/opt/postgresql@16/bin:$PATH"
fi

if [ -n "$PSQL_PATH" ]; then
    echo "Found PostgreSQL at: $PSQL_PATH"
    # Check if database exists
    if psql -lqt 2>/dev/null | cut -d \| -f 1 | grep -qw sl_db; then
        echo -e "${YELLOW}⚠ Database 'sl_db' already exists. Skipping creation...${NC}"
    else
        if createdb sl_db 2>/dev/null; then
            echo -e "${GREEN}✓ Database 'sl_db' created${NC}"
        else
            echo -e "${YELLOW}⚠ Could not create database. It may already exist or PostgreSQL is not running.${NC}"
            echo "  Try: brew services start postgresql@15"
        fi
    fi
    
    # Create raw schema
    if psql sl_db -c "CREATE SCHEMA IF NOT EXISTS raw;" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Schema 'raw' created${NC}"
    else
        echo -e "${YELLOW}⚠ Could not create schema. Database may not be accessible.${NC}"
    fi
else
    echo -e "${YELLOW}⚠ PostgreSQL (psql) not found.${NC}"
    echo "  Skipping database setup. You can set it up manually later:"
    echo "  macOS: brew install postgresql@15"
    echo "  Then: brew services start postgresql@15"
    echo "  Then: createdb sl_db && psql sl_db -c 'CREATE SCHEMA raw;'"
    echo ""
fi
echo ""

# Step 4: Configure dbt profiles
echo -e "${BLUE}Step 4: Configuring dbt profiles...${NC}"
mkdir -p ~/.dbt

if [ -f ~/.dbt/profiles.yml ]; then
    echo -e "${YELLOW}⚠ ~/.dbt/profiles.yml already exists.${NC}"
    read -p "Do you want to overwrite it? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Skipping profiles.yml copy...${NC}"
    else
        cp infrastructure/profiles.yml.template ~/.dbt/profiles.yml
        echo -e "${GREEN}✓ dbt profiles.yml copied${NC}"
        echo -e "${YELLOW}⚠ IMPORTANT: Update ~/.dbt/profiles.yml and replace POSTGRES_USERNAME with your PostgreSQL username${NC}"
    fi
else
    cp infrastructure/profiles.yml.template ~/.dbt/profiles.yml
    echo -e "${GREEN}✓ dbt profiles.yml copied${NC}"
    echo -e "${YELLOW}⚠ IMPORTANT: Update ~/.dbt/profiles.yml and replace POSTGRES_USERNAME with your PostgreSQL username${NC}"
    echo "  You can find your username by running: whoami"
    echo "  Or check: psql -l (look at the 'Owner' column)"
fi
echo ""

# Step 5: Configure Airflow paths
echo -e "${BLUE}Step 5: Configuring Airflow...${NC}"
export AIRFLOW_HOME=${AIRFLOW_HOME:-$PROJECT_ROOT/airflow}
echo -e "${GREEN}✓ AIRFLOW_HOME set to: $AIRFLOW_HOME${NC}"
echo ""

# Step 6: Install dbt packages
echo -e "${BLUE}Step 6: Installing dbt packages...${NC}"
cd dbt
dbt deps
cd ..
echo -e "${GREEN}✓ dbt packages installed${NC}"
echo ""

echo "=========================================="
echo -e "${GREEN}✓ Infrastructure Setup Complete!${NC}"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Update ~/.dbt/profiles.yml with your PostgreSQL username"
echo "2. Start Airflow: ./infrastructure/start_airflow.sh"
echo "3. Load data and run transformations: ./infrastructure/load_and_transform.sh"
echo ""

