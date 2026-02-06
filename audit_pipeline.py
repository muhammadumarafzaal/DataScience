"""
NYC CONGESTION PRICING FORENSIC AUDIT (2025) - MASTER ORCHESTRATOR
==================================================================

This module serves as the central nervous system for the "NYC Congestion 
Pricing Forensic Audit" pipeline. It synchronizes the multi-phase 
analytical workflow, ensuring data integrity and procedural consistency 
across the entire longitudinal study.

Operational Modes:
- Full Pipeline Execution: Automated sequential processing of all phases.
- Granular Phase Isolation: Execution of specific analytical segments.
- Cache-Aware Processing: Skips redundant data retrieval operations.

Analytical Lifecycle:
1. Data Harvest (Ingestion) -> 2. Schema Alignment (UDA) -> 3. Data Refinery (Cleaning) ->
4. SI-Model Imputation -> 5. Geospatial Forensics -> 6. Metric Synthesis ->
7. Visual Analytics -> 8. Climate Integration -> 9. Document Synthesis.
"""

import sys
import argparse
from pathlib import Path
from loguru import logger
from datetime import datetime

# Core System Integration
from src import settings
from src import raw_loader
from src import data_definitions
from src import ghost_trip_filter
from src import missing_value_handler
from src import geo_mapping
from src import fleet_analytics
from src import chart_generator
from src import climate_integration
from src import document_builder

# Operational Logging configuration
logger.remove()
logger.add(sys.stderr, format="{time:HH:mm:ss} | {level: <8} | {message}", level="INFO")
logger.add(
    settings.AUDIT_LOGS_DIR / f"audit_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"
)


def execute_phase_1_harvest():
    """Phase 1: Automated Data Harvest (Ingestion)"""
    logger.info("=" * 80)
    logger.info("PHASE 1: AUTOMATED DATA HARVEST (INGESTION)")
    logger.info("=" * 80)
    
    harvest_telemetry = raw_loader.execute_full_data_harvest(refresh_existing=False)
    
    failure_coefficient = harvest_telemetry['yellow_fail'] + harvest_telemetry['green_fail']
    if failure_coefficient > 0:
        logger.warning(f"Phase 1 concluded with {failure_coefficient} partition failures.")
        return False
    
    logger.success("Phase 1: Data harvest synchronization finalized.")
    return True


def execute_phase_2_schema_alignment():
    """Phase 2: Unified Data Architecture (Schema Alignment)"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 2: UNIFIED DATA ARCHITECTURE (SCHEMA ALIGNMENT)")
    logger.info("=" * 80)
    
    alignment_report = data_definitions.orchestrate_fleet_schema_alignment()
    
    if alignment_report['critical_failures'] > 0:
        logger.warning(f"Phase 2 concluded with {alignment_report['critical_failures']} structural defects.")
        return False
    
    logger.success("Phase 2: Fleet schema alignment finalized.")
    return True


def execute_phase_3_data_refinery():
    """Phase 3: Data Refinery (Cleaning & Anomaly Isolation)"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 3: DATA REFINERY (CLEANING & ANOMALY ISOLATION)")
    logger.info("=" * 80)
    
    refinery_metrics = ghost_trip_filter.process_refinery_batch()
    
    if refinery_metrics['partitions_processed'] == 0:
        logger.error("Phase 3 Aborted: No valid data partitions detected for refining.")
        return False
    
    # Forensic pattern analysis
    ghost_trip_filter.perform_behavioral_pattern_audit()
    
    logger.success("Phase 3: Data refinery and anomaly isolation finalized.")
    return True


def execute_phase_4_si_imputation():
    """Phase 4: Synthesis-Imputation (Missing Value Handling)"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 4: SYNTHESIS-IMPUTATION (MISSING VALUE HANDLING)")
    logger.info("=" * 80)
    
    missing_value_handler.run_comprehensive_data_recovery()
    
    logger.success("Phase 4: SI-Model temporal reconstruction finalized.")
    return True


def execute_phase_5_geospatial_forensics():
    """Phase 5: Geospatial Forensics (CBD Isolation & Categorization)"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 5: GEOSPATIAL FORENSICS (CBD ISOLATION & CATEGORIZATION)")
    logger.info("=" * 80)
    
    # Ingest spatial boundaries
    spatial_polygons = geo_mapping.ingest_geospatial_zone_polygons()
    
    if spatial_polygons is None:
        logger.error("Phase 5 Terminated: Spatial manifest ingestion failed.")
        return False
    
    # Isolate Policy Core
    cbd_centroids = geo_mapping.isolate_manhattan_cbd_centroids(spatial_polygons)
    
    if not cbd_centroids:
        logger.error("Phase 5 Terminated: Policy core centroids could not be identified.")
        return False
    
    # Execute categorization engine
    geo_mapping.execute_spatial_trip_categorization(cbd_centroids)
    geo_mapping.perform_regional_compliance_telemetry()
    
    logger.success("Phase 5: Geospatial categorization and telemetry finalized.")
    return True


def execute_phase_6_7_metric_synthesis():
    """Phase 6-7: Analytical Metric Synthesis (Compliance & Fleet Dynamics)"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 6-7: ANALYTICAL METRIC SYNTHESIS")
    logger.info("=" * 80)
    
    # Forensic Compliance Audit
    compliance_summary = fleet_analytics.conduct_operational_compliance_audit()
    
    # Inter-Fleet Behavioral Dynamics
    fleet_dynamics = fleet_analytics.evaluate_inter_fleet_dynamics()
    
    if not compliance_summary or not fleet_dynamics:
        logger.warning("Phase 6-7 concluded with incomplete analytical segments.")
        return False
    
    logger.success("Phase 6-7: Forensic metric synthesis finalized.")
    return True


def execute_phase_8_visual_analytics():
    """Phase 8: High-Fidelity Visual Analytics"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 8: HIGH-FIDELITY VISUAL ANALYTICS")
    logger.info("=" * 80)
    
    chart_generator.orchestrate_visual_audit_generation()
    
    logger.success("Phase 8: Visual artifact generation finalized.")
    return True


def execute_phase_9_climate_integration():
    """Phase 9: Environmental Impact & Climate Integration"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 9: ENVIRONMENTAL IMPACT & CLIMATE INTEGRATION")
    logger.info("=" * 80)
    
    climate_integration.synthesize_climate_demand_integrated_datamart()
    
    logger.success("Phase 9: Climatology demand synthesis finalized.")
    return True


def execute_phase_10_document_synthesis():
    """Phase 10: Institutional Audit Report Synthesis"""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 10: INSTITUTIONAL AUDIT REPORT SYNTHESIS")
    logger.info("=" * 80)
    
    document_builder.architect_comprehensive_audit_document()
    
    logger.success("Phase 10: Finalized audit artifact finalized.")
    return True


def orchestrate_holistic_audit_execution(skip_ingestion=False):
    """
    Coordinates the end-to-end execution of the forensic audit pipeline.
    
    Workflow Sequence:
    1 -> 2 -> 3 -> 4 -> 5 -> 6/7 -> 8 -> 9 -> 10.
    """
    
    logger.info("=" * 80)
    logger.info("NYC CONGESTION PRICING FORENSIC AUDIT 2025 - FULL PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Execution Commencement: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    # Initialize system warehouse infrastructure
    settings.initialize_audit_environment()
    
    # Execution Tracking Vector
    phase_success_matrix = {}
    
    # 1. Pipeline Start: Ingestion
    if not skip_ingestion:
        phase_success_matrix['Phase 1: Ingestion'] = execute_phase_1_harvest()
    else:
        logger.info("Bypassing Phase 1 (Data Harvest) via override.")
        phase_success_matrix['Phase 1: Ingestion'] = True
    
    # 2. Sequential Propagation
    if phase_success_matrix['Phase 1: Ingestion']:
        phase_success_matrix['Phase 2: Schema'] = execute_phase_2_schema_alignment()
        
        if phase_success_matrix['Phase 2: Schema']:
            phase_success_matrix['Phase 3: Refinery'] = execute_phase_3_data_refinery()
            
            if phase_success_matrix['Phase 3: Refinery']:
                phase_success_matrix['Phase 4: Imputation'] = execute_phase_4_si_imputation()
                
                if phase_success_matrix['Phase 4: Imputation']:
                    phase_success_matrix['Phase 5: Geospatial'] = execute_phase_5_geospatial_forensics()
                    
                    if phase_success_matrix['Phase 5: Geospatial']:
                        phase_success_matrix['Phase 6/7: Metrics'] = execute_phase_6_7_metric_synthesis()
                        
                        if phase_success_matrix['Phase 6/7: Metrics']:
                            phase_success_matrix['Phase 8: Visuals'] = execute_phase_8_visual_analytics()
                            
                            if phase_success_matrix['Phase 8: Visuals']:
                                phase_success_matrix['Phase 9: Climate'] = execute_phase_9_climate_integration()
                                
                                if phase_success_matrix['Phase 9: Climate']:
                                    phase_success_matrix['Phase 10: Reporting'] = execute_phase_10_document_synthesis()

    # Final Synthesis Reporting
    logger.info("\n" + "=" * 80)
    logger.info("ORCHESTRATION SUMMARY REPORT")
    logger.info("=" * 80)
    
    all_finalized = True
    for phase_name, was_successful in phase_success_matrix.items():
        outcome_label = "SENTINEL CHECK: ✓ SUCCESS" if was_successful else "SENTINEL CHECK: ✗ FAILED"
        logger.info(f"{phase_name:<30} | {outcome_label}")
        if not was_successful:
            all_finalized = False
    
    logger.info("=" * 80)
    logger.info(f"Execution Finalization: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    return all_finalized


def main():
    """
    Main entry point for command-line orchestration.
    """
    
    cmd_parser = argparse.ArgumentParser(
        description='NYC Congestion Pricing Forensic Audit Orchestrator'
    )
    
    cmd_parser.add_argument(
        '--phase', type=int, choices=range(1, 11),
        help='Isolate specific analytical phase (1-10)'
    )
    
    cmd_parser.add_argument(
        '--skip-harvest', action='store_true',
        help='Bypass Phase 1: Data Ingestion'
    )
    
    args = cmd_parser.parse_args()
    
    # Phase mapping for targeted execution
    targeted_execution_map = {
        1: execute_phase_1_harvest,
        2: execute_phase_2_schema_alignment,
        3: execute_phase_3_data_refinery,
        4: execute_phase_4_si_imputation,
        5: execute_phase_5_geospatial_forensics,
        6: execute_phase_6_7_metric_synthesis,
        7: execute_phase_6_7_metric_synthesis,
        8: execute_phase_8_visual_analytics,
        9: execute_phase_9_climate_integration,
        10: execute_phase_10_document_synthesis
    }
    
    if args.phase:
        was_successful = targeted_execution_map[args.phase]()
        sys.exit(0 if was_successful else 1)
    else:
        was_successful = orchestrate_holistic_audit_execution(skip_ingestion=args.skip_harvest)
        sys.exit(0 if was_successful else 1)


if __name__ == "__main__":
    main()
