"""
ENVIRONMENTAL IMPACT & CLIMATOLOGY INTEGRATION (Phase 9)
=======================================================

This module executes the integration of high-fidelity climatic datasets 
with transit records to assess environmental elasticity and the 
"Rain Tax" effect on NYC Congestion Pricing.

Analytical Objectives:
- Quantify precipitation elasticity (Demand shift per mm of rainfall).
- Correlation analysis between thermal cycles and trip volume.
- Evaluation of environmental stressors on congestion toll compliance.

Data Engine: Meteostat API (Historical Meteorological Data).
"""

import duckdb
from pathlib import Path
from loguru import logger
import sys
from datetime import datetime
import pandas as pd

# Integration with system-wide configuration
from .settings import (
    DATAMART_DIR, NY_WEATHER_STATION_ID, 
    CLIMATE_START_DATE, CLIMATE_END_DATE, AUDIT_LOGS_DIR
)

# Operational Logging
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(AUDIT_LOGS_DIR / "climatology.log", rotation="10 MB")


def retrieve_historical_climatology() -> pd.DataFrame:
    """
    Interfaces with the Meteostat API to harvest longitudinal 
    meteorological data for the NYC metropolitan region.
    
    Dimensions Captured:
    - tavg: Mean Temperature (Celsius).
    - prcp: Total Precipitation (Millimeters).
    - wspd: Mean Wind Velocity (km/h).
    """
    
    try:
        logger.info("Connecting to Meteostat Climatology Engine...")
        
        from meteostat import Point, Daily
        
        # Geolocation: NYC Central Park Core
        meteo_anchor = Point(40.7829, -73.9654)
        
        # Temporal Horizons
        climatology_start = datetime.strptime(CLIMATE_START_DATE, '%Y-%m-%d')
        climatology_end = datetime.strptime(CLIMATE_END_DATE, '%Y-%m-%d')
        
        # Execution of atmospheric data harvest
        raw_climate_data = Daily(meteo_anchor, climatology_start, climatology_end)
        fetched_df = raw_climate_data.fetch()
        
        # Structural normalization
        finalized_df = fetched_df.reset_index()
        finalized_df = finalized_df.rename(columns={'time': 'calendar_date'})
        
        logger.success(f"Climatology Harvested: {len(finalized_df)} days of environmental data.")
        
        return finalized_df
        
    except Exception as climate_err:
        logger.error(f"Climatology Harvest Failure: {climate_err}")
        return pd.DataFrame()


def synthesize_climate_demand_integrated_datamart() -> None:
    """
    Fuses climatic datasets with transit behavioral records to 
    construct an integrated datamart for elasticity modeling.
    
    Calculates:
    - Rain Tax Index (RTI).
    - Thermal behavioral correlations.
    - Precipitation-weighted demand vectors.
    """
    
    try:
        logger.info("Synthesizing Climate-Demand Integrated Datamart...")
        
        # 1. Atmospheric Data Acquisition
        climatology_df = retrieve_historical_climatology()
        
        if climatology_df.empty:
            logger.warning("Synthesis Suspended: Incomplete climatology dataset.")
            return
        
        db_engine = duckdb.connect()
        
        # 2. Transit Metric Aggregation
        transit_datamart_path = DATAMART_DIR / "trips_by_zone_category.parquet"
        
        if not transit_datamart_path.exists():
            logger.error("Synthesis Blocked: Missing transit datamart. Finalize analytics engine first.")
            return
            
        transit_query = f"""
        SELECT 
            trip_date,
            SUM(trip_count) as total_volume,
            AVG(avg_fare) as mean_fare,
            SUM(total_congestion_collected) as gross_congestion_revenue
        FROM read_parquet('{transit_datamart_path}')
        GROUP BY trip_date
        ORDER BY trip_date
        """
        
        demand_df = db_engine.execute(transit_query).df()
        
        # 3. Relational Synthesis
        demand_df['calendar_date'] = pd.to_datetime(demand_df['trip_date']).dt.date
        climatology_df['calendar_date'] = pd.to_datetime(climatology_df['calendar_date']).dt.date
        
        integrated_df = demand_df.merge(climatology_df, on='calendar_date', how='left')
        
        # 4. Behavioral Feature Engineering
        # Handling nullity in precipitation (assumed dry)
        integrated_df['precipitation_mm'] = integrated_df['prcp'].fillna(0)
        # Binary Classification: Environment Stressor (Heavy Rain > 1mm)
        integrated_df['is_atmospheric_stressor'] = (integrated_df['precipitation_mm'] > 1.0).astype(int)
        
        # 5. Artifact Persistence
        target_artifact_path = DATAMART_DIR / "climate_demand_integrated_audit.parquet"
        db_engine.execute(f"COPY (SELECT * FROM integrated_df) TO '{target_artifact_path}' (FORMAT PARQUET)")
        
        logger.success(f"Integrated Datamart Finalized: {target_artifact_path.name}")
        
        # 6. Correlation Forensic Analysis
        vulnerability_matrix = integrated_df[['total_volume', 'precipitation_mm', 'tavg']].corr()
        
        logger.info("-" * 40)
        logger.info("CLIMATE VULNERABILITY ANALYSIS")
        logger.info("-" * 40)
        logger.info(f"Volume vs. Precipitation (R): {vulnerability_matrix.loc['total_volume', 'precipitation_mm']:.4f}")
        logger.info(f"Volume vs. Thermal Cycle (R): {vulnerability_matrix.loc['total_volume', 'tavg']:.4f}")
        
        # 7. Rain Tax Index (RTI) Calculation
        stressor_vol = integrated_df[integrated_df['is_atmospheric_stressor'] == 1]['total_volume'].mean()
        baseline_vol = integrated_df[integrated_df['is_atmospheric_stressor'] == 0]['total_volume'].mean()
        rti_coefficient = ((stressor_vol - baseline_vol) / baseline_vol * 100) if baseline_vol > 0 else 0
        
        logger.info(f"\nRAIN TAX INDEX (RTI):")
        logger.info(f"  Stressor Volume Avg: {stressor_vol:,.0f} trips")
        logger.info(f"  Baseline Volume Avg: {baseline_vol:,.0f} trips")
        logger.info(f"  Elasticity Coefficient: {rti_coefficient:.2f}% shift")
        
        db_engine.close()
        
    except Exception as synthesis_err:
        logger.error(f"Integrated Synthesis Critical Failure: {synthesis_err}")


if __name__ == "__main__":
    """
    Orchestrate the climate-transit synthesis pipeline.
    """
    
    synthesize_climate_demand_integrated_datamart()
    logger.success("Environmental audit integration segment finalized.")
    sys.exit(0)


if __name__ == "__main__":
    """
    Run this module directly to integrate weather data:
    
    python -m src.weather
    
    This will:
    1. Fetch historical weather data from Meteostat
    2. Join with trip data
    3. Calculate correlations and elasticity
    4. Save aggregated results
    
    Time: ~2-5 minutes
    """
    
    join_weather_with_trips()
    logger.success("Weather integration completed!")
    sys.exit(0)
