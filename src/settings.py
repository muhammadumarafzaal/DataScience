"""
Auditing System Settings for NYC Congestion Pricing
===================================================

This module serves as the primary configuration engine for the NYC 
Congestion Pricing Audit. It defines global constants, system paths, 
and operational parameters used across the entire data pipeline.

Key Responsibilities:
- Environment Path Management
- Data Source Definitions
- Anomaly Detection Thresholds
- Architectural Mapping for Data Integration
"""

import os
from pathlib import Path
from datetime import datetime

# =========================================================================
# CORE DIRECTORY STRUCTURE
# =========================================================================

# The absolute root of the working repository
BASE_PROJECT_PATH = Path(__file__).parent.parent

# Storage containers for raw, interim, and processed data
DATA_WAREHOUSE = BASE_PROJECT_PATH / "data"
RAW_INGESTION_DIR = DATA_WAREHOUSE / "raw"
DATA_REFINERY_DIR = DATA_WAREHOUSE / "processed"
DATAMART_DIR = DATA_WAREHOUSE / "aggregated"

# Sub-directories for different taxi types (Raw)
VINTAGE_YELLOW_DIR = RAW_INGESTION_DIR / "yellow"
TRADITIONAL_GREEN_DIR = RAW_INGESTION_DIR / "green"
SPATIAL_ZONES_DIR = RAW_INGESTION_DIR / "taxi_zones"

# Sub-directories for refined datasets
CONSOLIDATED_DATA_DIR = DATA_REFINERY_DIR / "unified"
OUTLIER_TRIPS_DIR = DATA_REFINERY_DIR / "ghost_trips"

# Output and logging destinations
SYSTEM_EXPORTS = BASE_PROJECT_PATH / "outputs"
CHART_EXPORTS_DIR = SYSTEM_EXPORTS / "figures"
AUDIT_LOGS_DIR = SYSTEM_EXPORTS / "logs"

# =========================================================================
# EXTERNAL DATA PROVIDERS (NYC TLC)
# =========================================================================

# Official repository for NYC Taxi and Limousine Commission data
TLC_REMOTE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Standard naming convention for monthly data files
YELLOW_FILE_MASK = "yellow_tripdata_{year}-{month:02d}.parquet"
GREEN_FILE_MASK = "green_tripdata_{year}-{month:02d}.parquet"

# Geographic metadata for NYC taxi zones
ZONES_GEOMETRY_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

# =========================================================================
# AUDIT TIME HORIZONS
# =========================================================================

# Years required for comparative analysis and missing value estimation
AUDIT_YEARS = [2023, 2024, 2025]
CALENDAR_MONTHS = list(range(1, 13))

# Critical threshold: The official implementation of congestion pricing
POLICY_EFFECTIVE_DATE = datetime(2025, 1, 5)

# Benchmarking quarters for impact assessment
PRE_POLICY_QUARTER = [1, 2, 3]   # Q1 2024
POST_POLICY_QUARTER = [1, 2, 3]  # Q1 2025

# =========================================================================
# DATA INTEGRITY & ANOMALY DETECTION
# =========================================================================

# Speed limit logic: Identify trips exceeding realistic urban velocity
SUSPICIOUS_SPEED_LIMIT = 65.0  # Miles per hour

# Temporal constraints: Filter out "micro-trips" likely caused by GPS jitter
MINIMUM_TRIP_SECONDS = 60

# Financial audits: Flag short-haul trips with disproportionately high fares
REVENUE_ANOMALY_CAP = 20.0  # Dollars for short durations

# Distance safeguards: Ensure the vehicle actually moved
MINIMUM_MOVEMENT_MILES = 0.01

# =========================================================================
# CONGESTION ZONE SPATIAL LOGIC
# =========================================================================

# Defined implementation area: Manhattan Central Business District (CBD)
CBD_NORTH_BOUNDARY = "60th Street"

# =========================================================================
# UNIFIED DATA ARCHITECTURE (UDA)
# =========================================================================

# Standardized column structure for the unified analytics engine
CORE_ANALYTICS_SCHEMA = [
    "pickup_time",      # Chronological start
    "dropoff_time",     # Chronological end
    "pickup_loc",       # Origin Zone ID
    "dropoff_loc",      # Destination Zone ID
    "trip_distance",    # Verified distance in miles
    "fare",             # Base fare amount
    "total_amount",     # Gross revenue (including tips/tolls)
    "congestion_surcharge" # Specific policy-driven fee
]

# Mapping dictionary for Yellow Cab legacy fields
YELLOW_LEGACY_MAP = {
    "tpep_pickup_datetime": "pickup_time",
    "tpep_dropoff_datetime": "dropoff_time",
    "PULocationID": "pickup_loc",
    "DOLocationID": "dropoff_loc",
    "trip_distance": "trip_distance",
    "fare_amount": "fare",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge"
}

# Mapping dictionary for Green Cab legacy fields
GREEN_LEGACY_MAP = {
    "lpep_pickup_datetime": "pickup_time",
    "lpep_dropoff_datetime": "dropoff_time",
    "PULocationID": "pickup_loc",
    "DOLocationID": "dropoff_loc",
    "trip_distance": "trip_distance",
    "fare_amount": "fare",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge"
}

# =========================================================================
# STATISTICAL IMPUTATION MODEL (SI-Model)
# =========================================================================

# Weights for estimating missing December 2025 data based on historical cycles
# Model: Dec25_Est = (W1 * Dec23) + (W2 * Dec24)
HISTORICAL_CYCLE_WEIGHT = 0.3
RECENCY_TREND_WEIGHT = 0.7

# =========================================================================
# LOGGING FRAMEWORK
# =========================================================================

AUDIT_LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"
CURRENT_LOG_NAME = AUDIT_LOGS_DIR / f"audit_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# =========================================================================
# ENGINE OPTIMIZATION (DuckDB)
# =========================================================================

RESOURCES_MEMORY_CAP = "4GB"
PARALLEL_EXECUTION_THREADS = -1  # Maximum available

# =========================================================================
# CLIMATIC DATA SETTINGS
# =========================================================================

NY_WEATHER_STATION_ID = "72505"
CLIMATE_START_DATE = "2023-01-01"
CLIMATE_END_DATE = "2025-12-31"

# =========================================================================
# VISUALIZATION ENGINE SPECS
# =========================================================================

OPTIMAL_FIGURE_WD = 1200
OPTIMAL_FIGURE_HT = 600

# UI Theme Palette
PALETTE_HIGHLIGHT = "#FFC107"  # Yellow/Gold
PALETTE_SUCCESS = "#4CAF50"    # Green
PALETTE_CRITICAL = "#E91E63"   # Warning/Rose
PALETTE_BRAND = "#6366F1"      # Indigo (New Brand Color)

# =========================================================================
# SYSTEM INITIALIZATION UTILITIES
# =========================================================================

def initialize_audit_environment():
    """
    Constructs the necessary folder hierarchy for the audit system.
    Ensures all storage paths exist before processing begins.
    """
    required_directories = [
        RAW_INGESTION_DIR, VINTAGE_YELLOW_DIR, TRADITIONAL_GREEN_DIR, SPATIAL_ZONES_DIR,
        DATA_REFINERY_DIR, CONSOLIDATED_DATA_DIR, OUTLIER_TRIPS_DIR,
        DATAMART_DIR, SYSTEM_EXPORTS, CHART_EXPORTS_DIR, AUDIT_LOGS_DIR
    ]
    
    for folder in required_directories:
        folder.mkdir(parents=True, exist_ok=True)
    
    print("Environment Verification: All system paths are operational.")


if __name__ == "__main__":
    # Internal self-check for the settings engine
    print("-" * 60)
    print("SYSTEM CONFIGURATION UTILITY - NYC CONGESTION AUDIT")
    print("-" * 60)
    print(f"Base Repository: {BASE_PROJECT_PATH}")
    print(f"Data Warehouse: {DATA_WAREHOUSE}")
    print(f"Memory Cap: {RESOURCES_MEMORY_CAP}")
    print(f"Thread Allocation: {PARALLEL_EXECUTION_THREADS}")
    print(f"\nTarget Schema: {CORE_ANALYTICS_SCHEMA}")
    print("-" * 60)
    print("✓ Audit settings loaded successfully.")
