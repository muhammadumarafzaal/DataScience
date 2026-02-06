"""
UNIFIED DATA ARCHITECTURE (UDA) & SCHEMA ALIGNMENT (Phase 2)
===========================================================

This module orchestrates the structural alignment of disparate taxi transit 
datasets (Yellow Medallion and Green Medallion) into a singular, high-integrity 
Standardized Schema for downstream forensics.

Why is this essential?
- VINTAGE_YELLOW and TRADITIONAL_GREEN fleets utilize divergent column nomenclatures.
- A singular Analytical North Star is required for consistent longitudinal study.
- Out-of-core processing through DuckDB ensures the handling of multi-gigabyte 
  partitions without memory overflow.

Standardized Canonical Schema:
[pickup_time, dropoff_time, pickup_loc, dropoff_loc,
 trip_distance, fare, total_amount, congestion_surcharge]
"""

import duckdb
from pathlib import Path
from loguru import logger
import sys

# Integration with system-wide configuration
from .settings import (
    VINTAGE_YELLOW_DIR, TRADITIONAL_GREEN_DIR,
    CONSOLIDATED_DATA_DIR, YELLOW_LEGACY_MAP, GREEN_LEGACY_MAP,
    CORE_ANALYTICS_SCHEMA, AUDIT_LOGS_DIR
)

# Operational Logging
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(AUDIT_LOGS_DIR / "schema_alignment.log", rotation="10 MB")


def construct_structural_mapping_query(legacy_map: dict) -> str:
    """
    Constructs a finalized SQL SELECT statement that maps legacy 
    column headers to the Canonical Analytics Schema.
    
    Logic:
    - Iterates through the Core Analytics target headers.
    - Performs selective type casting (Timestamps, Doubles, Integers).
    - Injects NULL placeholders for non-existent source dimensions.
    """
    
    mapping_segments = []
    
    for target_col in CORE_ANALYTICS_SCHEMA:
        if target_col in legacy_map.values():
            # Identify the legacy identifier for this target
            legacy_id = [source_k for source_k, target_v in legacy_map.items() if target_v == target_col][0]
            
            # Dimensional-specific type casting for integrity
            if 'time' in target_col:
                mapping_segments.append(f"CAST({legacy_id} AS TIMESTAMP) AS {target_col}")
            elif target_col in ['trip_distance', 'fare', 'total_amount', 'congestion_surcharge']:
                mapping_segments.append(f"CAST({legacy_id} AS DOUBLE) AS {target_col}")
            elif 'loc' in target_col:
                mapping_segments.append(f"CAST({legacy_id} AS INTEGER) AS {target_col}")
            else:
                mapping_segments.append(f"{legacy_id} AS {target_col}")
        else:
            # Dimension missing in source; preserving schema with nullity
            mapping_segments.append(f"NULL AS {target_col}")
    
    return "SELECT " + ",\n       ".join(mapping_segments)


def synchronize_partition_schema(source_fragment: Path, destination_fragment: Path, source_map: dict) -> bool:
    """
    Executes a schema-aware transformation on a specific data partition.
    
    Performance Heuristics:
    - DuckDB streaming ensures the partition is processed out-of-core.
    - Columnar filtering minimizes I/O by only reading mapped legacy fields.
    - ZSTD compression is utilized for the finalized artifact storage.
    """
    
    try:
        logger.info(f"Synchronizing partition: {source_fragment.name}")
        
        db_engine = duckdb.connect()
        
        # Build the structural transformation query
        transformation_sql = construct_structural_mapping_query(source_map)
        
        # Execute the streaming schema-unification
        alignment_query = f"""
        COPY (
            {transformation_sql}
            FROM read_parquet('{source_fragment}')
            WHERE pickup_time IS NOT NULL 
              AND dropoff_time IS NOT NULL
              AND pickup_loc IS NOT NULL
              AND dropoff_loc IS NOT NULL
        )
        TO '{destination_fragment}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        
        db_engine.execute(alignment_query)
        
        # Verify throughput
        throughput_count = db_engine.execute(f"SELECT COUNT(*) FROM read_parquet('{destination_fragment}')").fetchone()[0]
        
        logger.success(f"Alignment Finalized: {source_fragment.name} ({throughput_count:,} records)")
        
        db_engine.close()
        return True
        
    except Exception as alignment_err:
        logger.error(f"Partition Synchronization Failure: {source_fragment.name} | {alignment_err}")
        return False


def orchestrate_fleet_schema_alignment() -> dict:
    """
    Automated coordinator for aligning the entire distributed fleet datasets.
    
    Process:
    1. Harvests all raw partitions from VINTAGE_YELLOW and TRADITIONAL_GREEN silos.
    2. Maps legacy headers to the Canonical Unified Schema.
    3. Persists results to the CONSOLIDATED_DATA infrastructure.
    """
    
    alignment_telemetry = {
        'yellow_aligned': 0, 'yellow_skipped': 0,
        'green_aligned': 0, 'green_skipped': 0,
        'critical_failures': 0
    }
    
    logger.info("=" * 70)
    logger.info("INITIATING FLEET SCHEMA ALIGNMENT (Phase 2)")
    logger.info("=" * 70)
    
    # Ensure target warehouse infrastructure exists
    CONSOLIDATED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Align Medallion Yellow Fleet
    logger.info("\nProcessing: Medallion Yellow Fleet partitions...")
    yellow_partitions = sorted(VINTAGE_YELLOW_DIR.glob("*.parquet"))
    
    for raw_file in yellow_partitions:
        target_file = CONSOLIDATED_DATA_DIR / f"yellow_unified_{raw_file.name}"
        
        if target_file.exists():
            logger.info(f"Skipping cached partition: {raw_file.name}")
            alignment_telemetry['yellow_skipped'] += 1
            continue
        
        if synchronize_partition_schema(raw_file, target_file, YELLOW_LEGACY_MAP):
            alignment_telemetry['yellow_aligned'] += 1
        else:
            alignment_telemetry['critical_failures'] += 1
    
    # 2. Align Medallion Green Fleet
    logger.info("\nProcessing: Medallion Green Fleet partitions...")
    green_partitions = sorted(TRADITIONAL_GREEN_DIR.glob("*.parquet"))
    
    for raw_file in green_partitions:
        target_file = CONSOLIDATED_DATA_DIR / f"green_unified_{raw_file.name}"
        
        if target_file.exists():
            logger.info(f"Skipping cached partition: {raw_file.name}")
            alignment_telemetry['green_skipped'] += 1
            continue
        
        if synchronize_partition_schema(raw_file, target_file, GREEN_LEGACY_MAP):
            alignment_telemetry['green_aligned'] += 1
        else:
            alignment_telemetry['critical_failures'] += 1
    
    # Report Alignment Telemetry
    logger.info("\n" + "=" * 70)
    logger.info("SCHEMA ALIGNMENT TELEMETRY")
    logger.info("=" * 70)
    logger.info(f"Yellow Fleet: {alignment_telemetry['yellow_aligned']} Aligned, {alignment_telemetry['yellow_skipped']} Skipped")
    logger.info(f"Green Fleet: {alignment_telemetry['green_aligned']} Aligned, {alignment_telemetry['green_skipped']} Skipped")
    if alignment_telemetry['critical_failures'] > 0:
        logger.error(f"Critical Alignment Failures: {alignment_telemetry['critical_failures']}")
    
    return alignment_telemetry


def validate_structural_integrity(artifact_path: Path) -> bool:
    """
    Performs a structural smoke-test to ensure the aligned artifact 
    conforms to the Core Analytics target schema.
    """
    
    try:
        db_engine = duckdb.connect()
        
        # Introspect artifact metadata
        structural_manifest = db_engine.execute(f"DESCRIBE SELECT * FROM read_parquet('{artifact_path}')").fetchall()
        observed_columns = [dimension[0] for dimension in structural_manifest]
        
        # Differential Integrity Check
        for required_dim in CORE_ANALYTICS_SCHEMA:
            if required_dim not in observed_columns:
                logger.error(f"Structural Defect: Missing dimension '{required_dim}' in {artifact_path.name}")
                return False
        
        logger.success(f"Structural Integrity Validated: {artifact_path.name}")
        db_engine.close()
        return True
        
    except Exception as validation_err:
        logger.error(f"Structural Verification Failure for {artifact_path.name}: {validation_err}")
        return False


if __name__ == "__main__":
    """
    Main entry point for explicit schema alignment orchestration.
    """
    
    # Execute batch alignment
    telemetry_report = orchestrate_fleet_schema_alignment()
    
    # Sample Validation
    sample_candidate = CONSOLIDATED_DATA_DIR / "yellow_unified_yellow_tripdata_2025-01.parquet"
    if sample_candidate.exists():
        validate_structural_integrity(sample_candidate)
    
    # Evaluation Logic
    if telemetry_report['critical_failures'] > 0:
        logger.error("Audit Blocked: Significant alignment failures detected.")
        sys.exit(1)
    else:
        logger.success("System Ready: All partitions aligned with Canonical Schema.")
        sys.exit(0)
