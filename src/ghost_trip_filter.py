"""
DATA REFINERY & ANOMALY DETECTION (Phase 2)
===========================================

This module implements the "Data Refinery" stage, where raw unified records 
are audited for structural integrity and logical consistency.

Primary Objective:
Detect and isolate "Outlier Trips" (formerly Ghost Trips) that exhibit 
characteristics of sensor failure, data corruption, or fraudulent activity.

Detection Heuristics:
1. Velocity Audit: Trips exceeding {SUSPICIOUS_SPEED_LIMIT} mph.
2. Temporal-Financial Audit: Ultra-short durations with excessive fares.
3. Spatial Audit: Zero-distance records with monetary charges.
4. Logical Guardrails: Negative durations or negative fare amounts.
"""

import duckdb
from pathlib import Path
from loguru import logger
import sys

# System settings integration
from .settings import (
    CONSOLIDATED_DATA_DIR, OUTLIER_TRIPS_DIR,
    SUSPICIOUS_SPEED_LIMIT, MINIMUM_TRIP_SECONDS, REVENUE_ANOMALY_CAP,
    MINIMUM_MOVEMENT_MILES, AUDIT_LOGS_DIR
)

# Operational Logging
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(AUDIT_LOGS_DIR / "ghost_trip_audit.log", rotation="10 MB")


def audit_and_filter_trips(source_parquet: Path, clean_output: Path, outlier_output: Path) -> dict:
    """
    Performs a deep-dive audit on a specific data partition and segregates 
    anomalous records from the primary analytics stream.
    
    Args:
        source_parquet: Raw unified dataset fragment.
        clean_output: Destination for verified records.
        outlier_output: Destination for records failing the audit.
    
    Returns:
        Refinery metrics summary.
    """
    
    try:
        logger.info(f"Refining dataset: {source_parquet.name}")
        
        # Initialize the high-performance analytical engine
        db_engine = duckdb.connect()
        
        # 1. Audit Staging & Logical Flagging
        refinery_query = f"""
        CREATE TEMP TABLE audit_staging_table AS
        SELECT *,
            -- Derived temporal metric for speed calculations
            EPOCH(dropoff_time - pickup_time) AS delta_t_seconds,
            
            -- Calculated spatial velocity
            CASE 
                WHEN EPOCH(dropoff_time - pickup_time) > 0 THEN
                    (trip_distance / (EPOCH(dropoff_time - pickup_time) / 3600.0))
                ELSE 
                    0
            END AS calculated_velocity_mph,
            
            -- Categorical refinery status tagging
            CASE
                -- CRITICAL: Unrealistic Velocity
                WHEN (trip_distance / (NULLIF(EPOCH(dropoff_time - pickup_time), 0) / 3600.0)) > {SUSPICIOUS_SPEED_LIMIT}
                    THEN 'STATUS_EXCESSIVE_VELOCITY'
                
                -- SUSPICIOUS: Micro-trip with outlier fare
                WHEN EPOCH(dropoff_time - pickup_time) < {MINIMUM_TRIP_SECONDS}
                     AND fare > {REVENUE_ANOMALY_CAP}
                    THEN 'STATUS_FINANCIAL_OUTLIER'
                
                -- IMPOSSIBLE: No movement with financial transaction
                WHEN trip_distance <= {MINIMUM_MOVEMENT_MILES}
                     AND fare > 0
                    THEN 'STATUS_SPATIAL_ANOMALY'
                
                -- LOGICAL: Temporal reversal (Time travel)
                WHEN EPOCH(dropoff_time - pickup_time) <= 0
                    THEN 'STATUS_TEMPORAL_ERROR'
                
                -- FINANCIAL: Negative revenue detection
                WHEN fare < 0 OR total_amount < 0
                    THEN 'STATUS_NEGATIVE_REVENUE'
                
                ELSE 'STATUS_VERIFIED'
            END AS refinery_status_flag
        FROM read_parquet('{source_parquet}')
        """
        
        db_engine.execute(refinery_query)
        
        # 2. Extract Verified Records
        verification_extraction = f"""
        COPY (
            SELECT pickup_time, dropoff_time, pickup_loc, dropoff_loc,
                   trip_distance, fare, total_amount, congestion_surcharge
            FROM audit_staging_table
            WHERE refinery_status_flag = 'STATUS_VERIFIED'
        )
        TO '{clean_output}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        db_engine.execute(verification_extraction)
        
        # 3. Secure Outlier Audit Trail
        outlier_preservation = f"""
        COPY (
            SELECT *, 
                   '{source_parquet.name}' AS lineage_source
            FROM audit_staging_table
            WHERE refinery_status_flag != 'STATUS_VERIFIED'
        )
        TO '{outlier_output}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        db_engine.execute(outlier_preservation)
        
        # 4. Synthesize Metrics
        metrical_analysis_sql = """
        SELECT 
            refinery_status_flag,
            COUNT(*) as record_count,
            ROUND(AVG(calculated_velocity_mph), 2) as mean_velocity,
            ROUND(AVG(fare), 2) as mean_fare
        FROM audit_staging_table
        GROUP BY refinery_status_flag
        ORDER BY record_count DESC
        """
        
        raw_metrics = db_engine.execute(metrical_analysis_sql).fetchall()
        
        # Transform results for reporting
        audit_results_map = {}
        for metric_row in raw_metrics:
            status, count, velocity, revenue = metric_row
            audit_results_map[status] = {
                'count': count,
                'mean_velocity': velocity,
                'mean_fare': revenue
            }
        
        # Operational Telemetry
        grand_total = sum(item['count'] for item in audit_results_map.values())
        verified_count = audit_results_map.get('STATUS_VERIFIED', {}).get('count', 0)
        outlier_count = grand_total - verified_count
        outlier_percent = (outlier_count / grand_total * 100) if grand_total > 0 else 0
        
        logger.info(f"Refinery Throughput: {grand_total:,} records")
        logger.info(f"Verified Yield: {verified_count:,} ({ (100 - outlier_percent):.2f}%)")
        logger.info(f"Refinery Waste: {outlier_count:,} ({outlier_percent:.2f}%)")
        
        db_engine.close()
        return audit_results_map
        
    except Exception as refinery_err:
        logger.error(f"Refinery failure on partition {source_parquet.name}: {refinery_err}")
        return {}


def process_refinery_batch() -> dict:
    """
    Main entry point for processing the entire unified data stream through 
    the refinery engine.
    """
    
    aggregate_reports = {
        'partitions_processed': 0,
        'gross_records': 0,
        'verified_records': 0,
        'anomaly_records': 0,
        'anomaly_distribution': {}
    }
    
    logger.info("=" * 70)
    logger.info("INITIATING BATCH DATA REFINERY")
    logger.info("=" * 70)
    
    # Ensure outlier storage exists
    OUTLIER_TRIPS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Discover all target unified partitions
    input_partitions = sorted(CONSOLIDATED_DATA_DIR.glob("*.parquet"))
    
    for partition_path in input_partitions:
        # Construct output filenames
        purified_path = CONSOLIDATED_DATA_DIR / partition_path.name.replace('unified', 'purified')
        outlier_trace_path = OUTLIER_TRIPS_DIR / partition_path.name.replace('unified', 'anomaly_trace')
        
        # Idempotency check
        if purified_path.exists():
            logger.info(f"Skipping cached partition: {partition_path.name}")
            continue
        
        # Execute individual audit
        partition_metrics = audit_and_filter_trips(partition_path, purified_path, outlier_trace_path)
        
        if partition_metrics:
            aggregate_reports['partitions_processed'] += 1
            
            for status_tag, data in partition_metrics.items():
                aggregate_reports['gross_records'] += data['count']
                
                if status_tag == 'STATUS_VERIFIED':
                    aggregate_reports['verified_records'] += data['count']
                else:
                    aggregate_reports['anomaly_records'] += data['count']
                    
                    if status_tag not in aggregate_reports['anomaly_distribution']:
                        aggregate_reports['anomaly_distribution'][status_tag] = 0
                    aggregate_reports['anomaly_distribution'][status_tag] += data['count']
    
    # Print Executive Summary
    logger.info("\n" + "=" * 70)
    logger.info("BATCH REFINERY EXECUTIVE SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Partitions Synchronized: {aggregate_reports['partitions_processed']}")
    logger.info(f"Gross Record Count: {aggregate_reports['gross_records']:,}")
    logger.info(f"Verified Records: {aggregate_reports['verified_records']:,}")
    logger.info(f"Identified Anomalies: {aggregate_reports['anomaly_records']:,}")
    
    if aggregate_reports['gross_records'] > 0:
        waste_yield = (aggregate_reports['anomaly_records'] / aggregate_reports['gross_records'] * 100)
        logger.info(f"System Waste Factor: {waste_yield:.2f}%")
    
    logger.info("\nAnomaly Breakdown:")
    for tag, val in aggregate_reports['anomaly_distribution'].items():
        logger.info(f"  {tag}: {val:,}")
    
    return aggregate_reports


def perform_behavioral_pattern_audit() -> None:
    """
    Aggregates anomaly metadata to identify systemic behavioral patterns in 
    the audited traffic data.
    """
    
    try:
        logger.info("\nSynthesizing behavioral anomaly patterns...")
        
        db_audit_conn = duckdb.connect()
        
        if not list(OUTLIER_TRIPS_DIR.glob("*.parquet")):
            logger.warning("Anomaly trace repository is empty. Skipping behavioral audit.")
            return
        
        pattern_sql = f"""
        SELECT 
            refinery_status_flag,
            COUNT(*) as occurrences,
            ROUND(AVG(calculated_velocity_mph), 2) as avg_velocity,
            ROUND(AVG(fare), 2) as avg_revenue,
            ROUND(AVG(trip_distance), 2) as avg_mileage
        FROM read_parquet('{OUTLIER_TRIPS_DIR}/*.parquet')
        GROUP BY refinery_status_flag
        ORDER BY occurrences DESC
        """
        
        patterns = db_audit_conn.execute(pattern_sql).fetchall()
        
        logger.info("\nLogical Anomaly Distribution:")
        logger.info("-" * 90)
        logger.info(f"{'Anomaly Category':<35} {'Count':>12} {'Velocity':>12} {'Revenue':>12} {'Mileage':>12}")
        logger.info("-" * 90)
        
        for p_row in patterns:
            tag, count, vel, rev, dist = p_row
            logger.info(f"{tag:<35} {count:>12,} {vel:>12.1f} {rev:>12.2f} {dist:>12.2f}")
        
        db_audit_conn.close()
        
    except Exception as pattern_err:
        logger.error(f"Behavioral audit failure: {pattern_err}")


if __name__ == "__main__":
    """
    Direct refinery orchestration for standalone dataset purification.
    """
    # Execute batch refinery
    refinery_stats = process_refinery_batch()
    
    # Deep dive into patterns
    perform_behavioral_pattern_audit()
    
    # Check for operational success
    if refinery_stats['partitions_processed'] > 0:
        logger.success("Refinery operations finalized successfully.")
        sys.exit(0)
    else:
        logger.error("No data partitions were available for refinement.")
        sys.exit(1)
