"""
STATISTICAL DATA RECOVERY & IMPUTATION (Phase 3)
===============================================

This module addresses temporal gaps in the dataset, specifically the 
projected data for December 2025.

Methodology:
Utilizes a "Weighted Cycle Imputation" model that synthesizes historical 
patterns (Dec 2023) with recent growth trends (Dec 2024).

The SI-Model (Statistical Imputation):
Formula: Est_2025 = (W_Cycle * Data_2023) + (W_Trend * Data_2024)
- Cycle Weight: {HISTORICAL_CYCLE_WEIGHT} (Captures seasonality)
- Trend Weight: {RECENCY_TREND_WEIGHT} (Captures economic trajectory)
"""

import duckdb
from pathlib import Path
from loguru import logger
import sys
from datetime import datetime

# System environment integration
from .settings import (
    CONSOLIDATED_DATA_DIR, HISTORICAL_CYCLE_WEIGHT, RECENCY_TREND_WEIGHT,
    AUDIT_LOGS_DIR
)

# Operational Logging Configuration
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(AUDIT_LOGS_DIR / "data_recovery.log", rotation="10 MB")


def verify_december_payload_presence() -> bool:
    """
    Scans the data warehouse to determine if physical records for 
    December 2025 have already been ingested.
    
    Returns:
        True if the data partition is already established.
    """
    
    yellow_horizon = list(CONSOLIDATED_DATA_DIR.glob("*yellow*2025-12*.parquet"))
    green_horizon = list(CONSOLIDATED_DATA_DIR.glob("*green*2025-12*.parquet"))
    
    is_present = len(yellow_horizon) > 0 and len(green_horizon) > 0
    
    if is_present:
        logger.info("Terminal Horizon Check: December 2025 records are physically present. Imputation bypassed.")
    else:
        logger.warning("Terminal Horizon Check: December 2025 is missing. Initiating SI-Model Recovery.")
    
    return is_present


def execute_cycle_imputation(fleet_category: str) -> bool:
    """
    Applies the weighted cycle imputation for a specific fleet (yellow/green).
    
    Process:
    1. Synchronizes historical December cycles.
    2. Aggregates behavioral metrics to the diary level.
    3. Synthesizes a virtual December partition using weighted vectors.
    """
    
    try:
        logger.info(f"SI-Model: Reconstructing December 2025 for {fleet_category} fleet...")
        
        db_session = duckdb.connect()
        
        # Identify historical cycle anchors
        historical_cycle_23 = list(CONSOLIDATED_DATA_DIR.glob(f"*{fleet_category}*2023-12*.parquet"))
        historical_cycle_24 = list(CONSOLIDATED_DATA_DIR.glob(f"*{fleet_category}*2024-12*.parquet"))
        
        if not historical_cycle_23 or not historical_cycle_24:
            logger.error(f"Recovery Failure: Insufficient historical anchors for {fleet_category}.")
            return False
        
        # Aggregate Cycle 2023 Behavioral Metrics
        cycle_23_sql = f"""
        CREATE TEMP TABLE daily_metrics_2023 AS
        SELECT 
            EXTRACT(DAY FROM pickup_time) as calendar_day,
            COUNT(*) as volume,
            AVG(fare) as mean_fare,
            AVG(total_amount) as mean_gross,
            AVG(trip_distance) as mean_distance,
            AVG(COALESCE(congestion_surcharge, 0)) as mean_surcharge,
            MODE(pickup_loc) as primary_origin,
            MODE(dropoff_loc) as primary_destination
        FROM read_parquet('{historical_cycle_23[0]}')
        GROUP BY calendar_day
        """
        db_session.execute(cycle_23_sql)
        
        # Aggregate Cycle 2024 Behavioral Metrics
        cycle_24_sql = f"""
        CREATE TEMP TABLE daily_metrics_2024 AS
        SELECT 
            EXTRACT(DAY FROM pickup_time) as calendar_day,
            COUNT(*) as volume,
            AVG(fare) as mean_fare,
            AVG(total_amount) as mean_gross,
            AVG(trip_distance) as mean_distance,
            AVG(COALESCE(congestion_surcharge, 0)) as mean_surcharge,
            MODE(pickup_loc) as primary_origin,
            MODE(dropoff_loc) as primary_destination
        FROM read_parquet('{historical_cycle_24[0]}')
        GROUP BY calendar_day
        """
        db_session.execute(cycle_24_sql)
        
        # Apply SI-Model Weighted Synthesis
        synthesis_sql = f"""
        CREATE TEMP TABLE synthesized_horizon_2025 AS
        SELECT 
            c23.calendar_day,
            CAST(
                {HISTORICAL_CYCLE_WEIGHT} * c23.volume + 
                {RECENCY_TREND_WEIGHT} * c24.volume 
            AS INTEGER) as estimated_volume,
            {HISTORICAL_CYCLE_WEIGHT} * c23.mean_fare + 
            {RECENCY_TREND_WEIGHT} * c24.mean_fare as estimated_fare,
            {HISTORICAL_CYCLE_WEIGHT} * c23.mean_gross + 
            {RECENCY_TREND_WEIGHT} * c24.mean_gross as estimated_gross,
            {HISTORICAL_CYCLE_WEIGHT} * c23.mean_distance + 
            {RECENCY_TREND_WEIGHT} * c24.mean_distance as estimated_distance,
            {HISTORICAL_CYCLE_WEIGHT} * c23.mean_surcharge + 
            {RECENCY_TREND_WEIGHT} * c24.mean_surcharge as estimated_surcharge,
            c24.primary_origin,
            c24.primary_destination
        FROM daily_metrics_2023 c23
        JOIN daily_metrics_2024 c24 ON c23.calendar_day = c24.calendar_day
        """
        db_session.execute(synthesis_sql)
        
        # Export virtual metrics to the data warehouse
        storage_dest = CONSOLIDATED_DATA_DIR / f"virtual_{fleet_category}_dec2025_metrics.parquet"
        
        persistence_sql = f"""
        COPY (
            SELECT 
                calendar_day,
                estimated_volume,
                estimated_fare,
                estimated_gross,
                estimated_distance,
                estimated_surcharge,
                primary_origin,
                primary_destination,
                '{HISTORICAL_CYCLE_WEIGHT}' as cycle_w,
                '{RECENCY_TREND_WEIGHT}' as trend_w,
                'SYNTHETIC_SI_MODEL' as record_ancestry
            FROM synthesized_horizon_2025
        )
        TO '{storage_dest}' (FORMAT PARQUET)
        """
        db_session.execute(persistence_sql)
        
        # Metrics Telemetry
        projected_volume = db_session.execute("SELECT SUM(estimated_volume) FROM synthesized_horizon_2025").fetchone()[0]
        logger.success(f"Recovery Success: Projected {projected_volume:,} virtual trips for {fleet_category} (Dec 2025).")
        
        db_session.close()
        return True
        
    except Exception as recovery_err:
        logger.error(f"SI-Model Failure for {fleet_category}: {recovery_err}")
        return False


def run_comprehensive_data_recovery() -> dict:
    """
    Orchestrates the statistical recovery process for all missing temporal data.
    """
    
    recovery_telemetry = {
        'horizon_gap_detected': False,
        'yellow_recovered': False,
        'green_recovered': False,
    }
    
    logger.info("=" * 70)
    logger.info("SYSTEM AUDIT - INITIATING DATA RECOVERY PHASE")
    logger.info("=" * 70)
    
    # 1. Horizon Scan
    if verify_december_payload_presence():
        logger.info("Audit Status: Terminal horizons are fully populated. No recovery needed.")
        return recovery_telemetry
    
    recovery_telemetry['horizon_gap_detected'] = True
    
    # 2. Yellow Fleet Imputation
    if execute_cycle_imputation('yellow'):
        recovery_telemetry['yellow_recovered'] = True
    
    # 3. Green Fleet Imputation
    if execute_cycle_imputation('green'):
        recovery_telemetry['green_recovered'] = True
    
    # Summary of Operations
    logger.info("\n" + "-" * 70)
    logger.info("RECOVERY OPERATION TRACKER")
    logger.info("-" * 70)
    logger.info(f"Gap Presence: {recovery_telemetry['horizon_gap_detected']}")
    logger.info(f"Yellow Matrix Recovery: {recovery_telemetry['yellow_recovered']}")
    logger.info(f"Green Matrix Recovery: {recovery_telemetry['green_recovered']}")
    
    return recovery_telemetry


if __name__ == "__main__":
    """
    Direct invocation of the SI-Model recovery engine.
    """
    
    system_recovery_status = run_comprehensive_data_recovery()
    
    # Validation
    if system_recovery_status['horizon_gap_detected']:
        if system_recovery_status['yellow_recovered'] and system_recovery_status['green_recovered']:
            logger.success("Critical Task: Missing temporal data has been synthetically restored.")
            sys.exit(0)
        else:
            logger.error("Critical Task: Data recovery failed to achieve full synchronization.")
            sys.exit(1)
    else:
        logger.success("Audit Status: Operational horizons are intact.")
        sys.exit(0)
