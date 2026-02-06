"""
GEOSPATIAL MAPPING & CONGESTION ZONE FORENSICS (Phase 5)
======================================================

This module executes the geospatial classification of taxi transit zones 
relative to the Manhattan Central Business District (CBD) Congestion 
Relief Zone.

Forensic Objectives:
- Identify zones south of 60th Street (CBD Core).
- Categorize trips based on CBD intersection (Entering, Exiting, Internal).
- Track surcharge compliance and leakage at a regional level.

Spatial Context:
The CBD Congestion Relief Zone covers Manhattan south of 60th Street, 
excluding major perimeter transit arteries. Compliance is mandatory for 
trips intersecting this boundary post-January 5, 2025.
"""

import duckdb
import geopandas as gpd
from pathlib import Path
from loguru import logger
import sys

# Integration with system-wide configuration
from .settings import (
    CONSOLIDATED_DATA_DIR, SPATIAL_ZONES_DIR, DATAMART_DIR,
    POLICY_EFFECTIVE_DATE, AUDIT_LOGS_DIR
)

# Operational Logging
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(AUDIT_LOGS_DIR / "geospatial_forensics.log", rotation="10 MB")


def ingest_geospatial_zone_polygons() -> gpd.GeoDataFrame:
    """
    Ingests official NYC TLC taxi zone shapefiles into a 
    GeoDataFrame for spatial introspection.
    
    Attributes:
    - LocationID: Canonical zone identifier.
    - zone: Neighborhood nomenclature.
    - borough: Macro-regional classification.
    """
    
    try:
        geometry_manifest = SPATIAL_ZONES_DIR / "taxi_zones.shp"
        
        if not geometry_manifest.exists():
            logger.error(f"Geospatial Manifest Missing: {geometry_manifest}")
            return None
        
        logger.info("Loading taxi zone geospatial polygons...")
        zone_polygons = gpd.read_file(geometry_manifest)
        
        logger.success(f"Geospatial Load Finalized: {len(zone_polygons)} zones ingested.")
        return zone_polygons
        
    except Exception as geo_err:
        logger.error(f"Geospatial Ingestion Failure: {geo_err}")
        return None


def isolate_manhattan_cbd_centroids(zone_polygons: gpd.GeoDataFrame) -> list:
    """
    Isolates taxi zones that fall within the Manhattan Central 
    Business District (CBD) south of 60th Street.
    
    Heuristic:
    - Initial borough-level filtering (Manhattan).
    - Attribute-based matching for known CBD neighborhoods.
    """
    
    try:
        logger.info("Isolating CBD Congestion Relief Zone segments...")
        
        # Micro-regional filter: Manhattan Legacy zones
        manhattan_segments = zone_polygons[zone_polygons['borough'] == 'Manhattan'].copy()
        
        # Forensic keywords for CBD identification
        cbd_nomenclature = [
            'Financial', 'Battery', 'Tribeca', 'SoHo', 'Chinatown',
            'Lower East Side', 'East Village', 'West Village', 'Greenwich',
            'Chelsea', 'Gramercy', 'Murray Hill', 'Midtown', 'Clinton',
            'Garment', 'Times Square', 'Penn Station', 'Flatiron'
        ]
        
        # Extraction of matching zones
        cbd_segments = manhattan_segments[
            manhattan_segments['zone'].str.contains('|'.join(cbd_nomenclature), case=False, na=False)
        ]
        
        cbd_zone_ids = cbd_segments['LocationID'].tolist()
        
        logger.success(f"CBD Isolation Finalized: {len(cbd_zone_ids)} zones identified as Policy-Critical.")
        return cbd_zone_ids
        
    except Exception as isolation_err:
        logger.error(f"CBD Isolation Failure: {isolation_err}")
        return []


def execute_spatial_trip_categorization(cbd_zone_identifiers: list) -> None:
    """
    Orchestrates the categorization of trips based on their 
    spatial interaction with the CBD boundary.
    
    Categories:
    - 'inside_zone': Static internal movements.
    - 'entering_zone': CBD Inbound (Critical for Toll).
    - 'exiting_zone': CBD Outbound (Critical for Toll).
    - 'outside_zone': External transit.
    """
    
    try:
        logger.info("Executing spatial trip categorization...")
        
        db_engine = duckdb.connect()
        
        # Structural conversion of identifiers
        cbd_list_sql = ','.join(map(str, cbd_zone_identifiers))
        
        # Verification of refined datasets
        refined_partitions = list(CONSOLIDATED_DATA_DIR.glob("*clean*.parquet"))
        
        if not refined_partitions:
            logger.warning("Categorization Suspended: No refined data partitions detected.")
            return
        
        # Phase 1: Spatial Categorization Staging
        categorization_sql = f"""
        CREATE TEMP TABLE spatial_audit_staging AS
        SELECT 
            *,
            -- Binary spatial flags
            CASE WHEN pickup_loc IN ({cbd_list_sql}) THEN 1 ELSE 0 END as is_origin_cbd,
            CASE WHEN dropoff_loc IN ({cbd_list_sql}) THEN 1 ELSE 0 END as is_dest_cbd,
            
            -- Categorical transition tagging
            CASE 
                WHEN pickup_loc IN ({cbd_list_sql}) AND dropoff_loc IN ({cbd_list_sql}) 
                    THEN 'inside_zone'
                WHEN pickup_loc NOT IN ({cbd_list_sql}) AND dropoff_loc IN ({cbd_list_sql}) 
                    THEN 'entering_zone'
                WHEN pickup_loc IN ({cbd_list_sql}) AND dropoff_loc NOT IN ({cbd_list_sql}) 
                    THEN 'exiting_zone'
                ELSE 'outside_zone'
            END as zone_category,
            
            -- Policy temporal correlation
            CASE WHEN pickup_time >= '{POLICY_EFFECTIVE_DATE}' THEN 1 ELSE 0 END as after_congestion_start
            
        FROM read_parquet('{CONSOLIDATED_DATA_DIR}/*clean*.parquet')
        """
        
        db_engine.execute(categorization_sql)
        
        # Phase 2: Datamart Persistence
        target_datamart_path = DATAMART_DIR / "trips_by_zone_category.parquet"
        
        persistence_sql = f"""
        COPY (
            SELECT 
                DATE_TRUNC('day', pickup_time) as trip_date,
                zone_category,
                after_congestion_start,
                COUNT(*) as trip_count,
                AVG(fare) as avg_fare,
                AVG(total_amount) as avg_total,
                AVG(trip_distance) as avg_distance,
                SUM(COALESCE(congestion_surcharge, 0)) as total_congestion_collected,
                -- Forensic Compliance: Ratio of surcharge capture
                SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) as trips_with_surcharge,
                SUM(CASE WHEN congestion_surcharge IS NULL OR congestion_surcharge = 0 THEN 1 ELSE 0 END) as trips_without_surcharge
            FROM spatial_audit_staging
            GROUP BY trip_date, zone_category, after_congestion_start
        )
        TO '{target_datamart_path}' (FORMAT PARQUET)
        """
        
        db_engine.execute(persistence_sql)
        
        # Forensic Reporting
        summary_analytics_sql = """
        SELECT 
            zone_category,
            after_congestion_start,
            COUNT(*) as record_volume,
            AVG(COALESCE(congestion_surcharge, 0)) as mean_surcharge
        FROM spatial_audit_staging
        GROUP BY zone_category, after_congestion_start
        ORDER BY zone_category, after_congestion_start
        """
        
        audit_summary = db_engine.execute(summary_analytics_sql).fetchall()
        
        logger.info("\nSPATIAL CATEGORIZATION AUDIT SUMMARY")
        logger.info("=" * 70)
        logger.info(f"{'Spatial Category':<20} {'Policy Phase':<15} {'Record Volume':>15} {'Mean Surcharge':>15}")
        logger.info("-" * 70)
        
        for record in audit_summary:
            cat, phase_bit, vol, mean_sur = record
            phase_label = "Post-Policy" if phase_bit else "Pre-Baseline"
            logger.info(f"{cat:<20} {phase_label:<15} {vol:>15,} ${mean_sur:>14.2f}")
        
        logger.success(f"Spatial datamart persisted: {target_datamart_path.name}")
        
        db_engine.close()
        
    except Exception as categorization_err:
        logger.error(f"Spatial Categorization Failure: {categorization_err}")


def perform_regional_compliance_telemetry() -> None:
    """
    Performs high-granularity compliance telemetry at 
    the pickup-location level for CBD-inbound transit.
    """
    
    try:
        logger.info("Initiating regional compliance telemetry...")
        
        db_engine = duckdb.connect()
        
        telemetry_artifact_path = DATAMART_DIR / "regional_compliance_telemetry.parquet"
        
        telemetry_sql = f"""
        COPY (
            SELECT 
                pickup_loc,
                COUNT(*) as total_records,
                AVG(fare) as mean_fare,
                SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) as compliant_records,
                SUM(CASE WHEN congestion_surcharge IS NULL OR congestion_surcharge = 0 THEN 1 ELSE 0 END) as leakage_records,
                ROUND(100.0 * SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as compliance_coefficient
            FROM read_parquet('{CONSOLIDATED_DATA_DIR}/*clean*.parquet')
            WHERE pickup_time >= '{POLICY_EFFECTIVE_DATE}'
              AND dropoff_loc IN (SELECT DISTINCT pickup_loc FROM read_parquet('{DATAMART_DIR}/trips_by_zone_category.parquet'))
            GROUP BY pickup_loc
            ORDER BY total_records DESC
            LIMIT 250
        )
        TO '{telemetry_artifact_path}' (FORMAT PARQUET)
        """
        
        db_engine.execute(telemetry_sql)
        logger.success(f"Compliance telemetry artifact finalized: {telemetry_artifact_path.name}")
        
        db_engine.close()
        
    except Exception as telemetry_err:
        logger.error(f"Regional Telemetry Critical Failure: {telemetry_err}")


if __name__ == "__main__":
    """
    Main execution pipeline for geospatial audit operations.
    """
    
    # Initialize Datamart Infrastructure
    DATAMART_DIR.mkdir(parents=True, exist_ok=True)
    
    # Ingest Geospatial Boundaries
    zone_polygons = ingest_geospatial_zone_polygons()
    
    if zone_polygons is not None:
        # Isolate Policy Area
        cbd_identifiers = isolate_manhattan_cbd_centroids(zone_polygons)
        
        if cbd_identifiers:
            # Perform Spatial Analytics
            execute_spatial_trip_categorization(cbd_identifiers)
            perform_regional_compliance_telemetry()
            
            logger.success("Geospatial forensic pipeline execution complete.")
            sys.exit(0)
        else:
            logger.error("Audit Terminated: Could not isolate CBD boundaries.")
            sys.exit(1)
    else:
        logger.error("Audit Terminated: Geospatial ingestion error.")
        sys.exit(1)


if __name__ == "__main__":
    """
    Run this module directly to filter congestion zones:
    
    python -m src.zones
    
    This will:
    1. Load taxi zone shapefiles
    2. Identify zones south of 60th Street
    3. Classify all trips by zone category
    4. Analyze patterns
    5. Save aggregated results
    
    Time: ~10-20 minutes
    """
    
    # Create output directory
    AGGREGATED_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load taxi zones
    zones = load_taxi_zones()
    
    if zones is not None:
        # Identify congestion zones
        congestion_zone_ids = identify_congestion_zones(zones)
        
        if congestion_zone_ids:
            # Classify trips
            classify_trips_by_zone(congestion_zone_ids)
            
            # Analyze patterns
            analyze_zone_patterns()
            
            logger.success("Congestion zone analysis completed!")
            sys.exit(0)
        else:
            logger.error("Failed to identify congestion zones")
            sys.exit(1)
    else:
        logger.error("Failed to load taxi zones")
        sys.exit(1)
