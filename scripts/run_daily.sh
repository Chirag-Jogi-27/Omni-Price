#!/usr/bin/env bash
# ==============================================================================
# OmniPrice Daily Pipeline Runner
#
# Sequential execution:
# 1. Amazon Search Spider: crawls FMCG categories, indexes ASINs into CatalogTracker
# 2. Amazon PDP Spider: crawls full product specifications (new ASINs + rolling 7-day refresh)
# 3. Volume Uploader: syncs today's raw JSONL & manifests to Databricks Unity Catalog Volume
#
# Databricks Lakeflow Job (Bronze -> Silver -> dbt Gold) triggers automatically
# upon arrival of files in the volume.
# ==============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Ensure logs directory exists
mkdir -p "$PROJECT_ROOT/logs"
RUN_ID=$(date +'%Y%m%d_%H%M%S')
LOG_FILE="$PROJECT_ROOT/logs/daily_run_${RUN_ID}.log"

# Log both to console and file
exec > >(tee -a "$LOG_FILE") 2>&1

echo "========================================================================"
echo "🚀 [OmniPrice] Starting Daily Pipeline Run: ${RUN_ID}"
echo "   Working Directory: ${PROJECT_ROOT}"
echo "   Log File: ${LOG_FILE}"
echo "========================================================================"

# Activate Virtual Environment
if [ -d "$PROJECT_ROOT/venv" ]; then
    echo "📦 Activating virtual environment..."
    source "$PROJECT_ROOT/venv/bin/activate"
else
    echo "⚠️ venv not found at $PROJECT_ROOT/venv, using system python3"
fi

# Set PYTHONPATH so 'core' and other modules resolve from root
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"

START_TIME=$(date +%s)

# ------------------------------------------------------------------------------
# STEP 1: Run Search Spider
# ------------------------------------------------------------------------------
echo ""
echo "------------------------------------------------------------------------"
echo "🛒 STEP 1: Running Amazon FMCG Search Spider..."
echo "------------------------------------------------------------------------"
python3 -m scrapers.amazon_spider --pages 10

# ------------------------------------------------------------------------------
# STEP 2: Run PDP Spider (New ASINs + Priority Queue Deep-Dive)
# ------------------------------------------------------------------------------
echo ""
echo "------------------------------------------------------------------------"
echo "🔍 STEP 2: Running Amazon PDP Spider (Spec Deep-Dive)..."
echo "------------------------------------------------------------------------"
python3 -m scrapers.amazon_pdp_spider --mode spec --limit 2000 --max-age-days 7

# ------------------------------------------------------------------------------
# STEP 3: Sync Raw Data to Databricks Unity Catalog Volume
# ------------------------------------------------------------------------------
echo ""
echo "------------------------------------------------------------------------"
echo "☁️ STEP 3: Uploading Daily Scraped Data to Databricks Volume..."
echo "------------------------------------------------------------------------"
python3 -m scripts.upload_to_volume

# ------------------------------------------------------------------------------
# STEP 4: Live Catalog Status Report
# ------------------------------------------------------------------------------
echo ""
echo "------------------------------------------------------------------------"
echo "📊 STEP 4: Generating Live Diagnostic Report..."
echo "------------------------------------------------------------------------"
python3 -m scripts.report_catalog_status

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

echo ""
echo "========================================================================"
echo "🎉 [OmniPrice] Daily Pipeline Completed Successfully in ${TOTAL_DURATION}s"
echo "   Databricks Volume has received today's raw data."
echo "   Databricks Lakeflow Job will automatically trigger Medallion pipeline."
echo "========================================================================"
