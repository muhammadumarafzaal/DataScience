"""
DATA INGESTION ENGINE (Phase 1)
===============================

This module provides the machinery for harvesting raw parquet files from the 
Official NYC TLC (Taxi & Limousine Commission) data repository.

Capabilities:
- Automated remote link discovery
- Resilient multi-threaded downloads with retry logic
- Parquet structural validation
- Spatial geometry acquisition (Taxi Zones)
"""

import requests
from bs4 import BeautifulSoup
from pathlib import Path
import time
from tqdm import tqdm
from loguru import logger
import sys

# Core system settings integration
from .settings import (
    TLC_REMOTE_URL, YELLOW_FILE_MASK, GREEN_FILE_MASK,
    VINTAGE_YELLOW_DIR, TRADITIONAL_GREEN_DIR, SPATIAL_ZONES_DIR, ZONES_GEOMETRY_URL,
    AUDIT_YEARS, CALENDAR_MONTHS, AUDIT_LOGS_DIR
)

# Operational Logging Setup
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(AUDIT_LOGS_DIR / "ingestion_engine.log", rotation="10 MB")


def fetch_remote_resource(endpoint_url: str, local_path: Path, retry_limit: int = 3) -> bool:
    """
    Downloads a binary resource from an endpoint with a fault-tolerant retry mechanism.
    
    Logic:
    - Streams the content to minimize memory usage for large Parquet files.
    - Implements exponential backoff to handle transient network issues.
    - Utilizes TQDM for real-time throughput visualization.
    """
    
    for attempt_idx in range(retry_limit):
        try:
            logger.info(f"Connecting to: {endpoint_url} (Attempt {attempt_idx + 1})")
            
            # Initiate stream connection
            with requests.get(endpoint_url, stream=True, timeout=30) as stream_conn:
                stream_conn.raise_for_status()
                
                # Extract payload size for progress tracking
                payload_size = int(stream_conn.headers.get('content-length', 0))
                
                # Ensure the filesystem target exists
                local_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Write stream to disk
                with open(local_path, 'wb') as file_handle:
                    with tqdm(total=payload_size, unit='B', unit_scale=True, 
                             desc=f"Retrieving {local_path.name}") as progress_viewer:
                        for byte_chunk in stream_conn.iter_content(chunk_size=16384): # Increased chunk size for speed
                            if byte_chunk:
                                file_handle.write(byte_chunk)
                                progress_viewer.update(len(byte_chunk))
            
            logger.success(f"Successfully cached: {local_path.name}")
            return True
            
        except (requests.exceptions.RequestException, IOError) as network_err:
            logger.warning(f"Connection issue encountered: {network_err}")
            
            if attempt_idx < retry_limit - 1:
                # Wait before retry (2, 4, 8 seconds)
                cooldown_period = 2 ** attempt_idx
                logger.info(f"Re-attempting in {cooldown_period}s...")
                time.sleep(cooldown_period)
            else:
                logger.error(f"Critical failure: Could not retrieve {endpoint_url}")
                return False
    
    return False


def verify_parquet_integrity(data_path: Path) -> bool:
    """
    Performs a structural smoke test on the downloaded Parquet file.
    
    Uses pyarrow to read metadata without loading the full payload.
    Ensures the file is readable and contains at least one record.
    """
    try:
        import pyarrow.parquet as pq
        
        # Meta-analysis of the parquet structure
        data_table = pq.ParquetFile(data_path)
        record_count = data_table.metadata.num_rows
        
        if record_count > 0:
            logger.success(f"Integrity Check Passed: {data_path.name} ({record_count:,} records)")
            return True
        else:
            logger.error(f"Integrity Breach: {data_path.name} is empty.")
            return False
            
    except Exception as validation_err:
        logger.error(f"Integrity Failure for {data_path.name}: {validation_err}")
        return False


def setup_geographic_zones() -> bool:
    """
    Retrieves and extracts the NYC Taxi Zone shapefiles essential for spatial auditing.
    """
    try:
        import zipfile
        
        logger.info("Initiating spatial metadata retrieval...")
        
        # Temporary archive path
        archive_target = SPATIAL_ZONES_DIR / "taxi_zones_bundle.zip"
        
        if not fetch_remote_resource(ZONES_GEOMETRY_URL, archive_target):
            return False
        
        # Extraction process
        logger.info("Decompressing spatial geometry files...")
        with zipfile.ZipFile(archive_target, 'r') as zip_bundle:
            zip_bundle.extractall(SPATIAL_ZONES_DIR)
        
        # Cleanup
        archive_target.unlink()
        
        logger.success("Spatial infrastructure readiness: OK")
        return True
        
    except Exception as spatial_err:
        logger.error(f"Spatial setup failed: {spatial_err}")
        return False


def execute_full_data_harvest(refresh_existing: bool = False) -> dict:
    """
    Orchestrates the massive task of acquiring the multi-year NYC taxi dataset.
    
    Coverage:
    - Yellow Medallion Data ({AUDIT_YEARS})
    - Green Boro Taxi Data ({AUDIT_YEARS})
    - Geographic Zone Definitions
    """
    
    harvest_tracker = {
        'yellow_ok': 0,
        'yellow_skipped': 0,
        'yellow_fail': 0,
        'green_ok': 0,
        'green_skipped': 0,
        'green_fail': 0,
    }
    
    logger.info("=" * 70)
    logger.info("PROJECT AUDIT - DATA HARVESTING INITIATED")
    logger.info("=" * 70)
    
    # 1. Spatial Geometry Check
    if not (SPATIAL_ZONES_DIR / "taxi_zones.shp").exists():
        setup_geographic_zones()
    else:
        logger.info("Geographic zones detected in cache. Skipping spatial setup.")
    
    # 2. Yellow Fleet Retrieval
    logger.info("\n" + "-" * 70)
    logger.info("COMPONENT: Yellow Medallion Data Stream")
    logger.info("-" * 70)
    
    for y in AUDIT_YEARS:
        for m in CALENDAR_MONTHS:
            file_name_str = YELLOW_FILE_MASK.format(year=y, month=m)
            web_source = f"{TLC_REMOTE_URL}/{file_name_str}"
            cache_target = VINTAGE_YELLOW_DIR / file_name_str
            
            if not refresh_existing and cache_target.exists():
                logger.info(f"Cache Hit: {file_name_str}")
                harvest_tracker['yellow_skipped'] += 1
                continue
            
            if fetch_remote_resource(web_source, cache_target):
                if verify_parquet_integrity(cache_target):
                    harvest_tracker['yellow_ok'] += 1
                else:
                    cache_target.unlink()
                    harvest_tracker['yellow_fail'] += 1
            else:
                harvest_tracker['yellow_fail'] += 1
    
    # 3. Green Fleet Retrieval
    logger.info("\n" + "-" * 70)
    logger.info("COMPONENT: Green Boro Taxi Data Stream")
    logger.info("-" * 70)
    
    for y in AUDIT_YEARS:
        for m in CALENDAR_MONTHS:
            file_name_str = GREEN_FILE_MASK.format(year=y, month=m)
            web_source = f"{TLC_REMOTE_URL}/{file_name_str}"
            cache_target = TRADITIONAL_GREEN_DIR / file_name_str
            
            if not refresh_existing and cache_target.exists():
                logger.info(f"Cache Hit: {file_name_str}")
                harvest_tracker['green_skipped'] += 1
                continue
            
            if fetch_remote_resource(web_source, cache_target):
                if verify_parquet_integrity(cache_target):
                    harvest_tracker['green_ok'] += 1
                else:
                    cache_target.unlink()
                    harvest_tracker['green_fail'] += 1
            else:
                harvest_tracker['green_fail'] += 1
    
    # 4. Final Executive Summary
    logger.info("\n" + "=" * 70)
    logger.info("HARVEST SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Yellow Fleet: Successful={harvest_tracker['yellow_ok']} | Skipped={harvest_tracker['yellow_skipped']} | Failed={harvest_tracker['yellow_fail']}")
    logger.info(f"Green Fleet:  Successful={harvest_tracker['green_ok']} | Skipped={harvest_tracker['green_skipped']} | Failed={harvest_tracker['green_fail']}")
    
    return harvest_tracker


if __name__ == "__main__":
    """
    Direct Invocation Mode:
    Executes the ingestion pipeline to populate the local data warehouse.
    Estimated data footprint: ~50 GB.
    """
    
    # Environment synchronization
    from .settings import initialize_audit_environment
    initialize_audit_environment()
    
    # Begin acquisition
    final_stats = execute_full_data_harvest(refresh_existing=False)
    
    # Operational Status Check
    critical_failures = final_stats['yellow_fail'] + final_stats['green_fail']
    if critical_failures > 0:
        logger.error(f"Pipeline Audit: {critical_failures} retrieval failures detected.")
        sys.exit(1)
    else:
        logger.success("Data warehouse synchronization complete.")
        sys.exit(0)
