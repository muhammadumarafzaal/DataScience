"""
POLICY IMPACT METRICS & COMPLIANCE ENGINE (Phase 4-5)
=====================================================

This module serves as the primary analytical core for assessing the 
socio-economic and operational consequences of the NYC Congestion Pricing.

Core Analytical Streams:
1. OPERATIONAL COMPLIANCE AUDIT:
   - Identifies "Leakage" (Cross-border trips missing the required surcharge).
   - Calculates theoretical vs. actual revenue capture.
   - Pinpoints geographical hotspots for toll non-compliance.

2. INTER-FLEET DYNAMICS EVALUATION:
   - Comparative longitudinal study (Q1 2024 vs Q1 2025).
   - Quantifies market share shifts between Yellow and Green medallions.
   - Measures behavioral elasticity in response to financial policy triggers.
"""

import duckdb
from pathlib import Path
from loguru import logger
import sys

# System-wide configuration integration
from .settings import (
    DATAMART_DIR, POLICY_EFFECTIVE_DATE,
    PRE_POLICY_QUARTER, POST_POLICY_QUARTER, AUDIT_LOGS_DIR
)

# Operational Logging
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(AUDIT_LOGS_DIR / "fleet_performance.log", rotation="10 MB")


def conduct_operational_compliance_audit() -> dict:
    """
    Executes a forensic audit of the congestion surcharge application 
    across all cross-border trip records post-policy-implementation.
    
    Leakage Definition:
    High-integrity records that entered or exited the Central Business 
    District (CBD) after {POLICY_EFFECTIVE_DATE} but lack a valid 
    congestion surcharge entry.
    """
    
    try:
        logger.info("=" * 70)
        logger.info("INITIATING OPERATIONAL COMPLIANCE AUDIT")
        logger.info("=" * 70)
        
        db_audit_engine = duckdb.connect()
        
        # Access the zone-categorical datamart
        zone_datamart_path = DATAMART_DIR / "trips_by_zone_category.parquet"
        
        if not zone_datamart_path.exists():
            logger.error("Audit Blocked: Missing zone classification datamart. Ensure geo_mapping is finalized.")
            return {}
        
        # Calculate Forensic Compliance Metrics
        compliance_sql = f"""
        SELECT 
            SUM(trip_count) as total_observed_volume,
            SUM(trips_with_surcharge) as compliant_volume,
            SUM(trips_without_surcharge) as non_compliant_leakage,
            ROUND(100.0 * SUM(trips_without_surcharge) / SUM(trip_count), 2) as leakage_coefficient,
            SUM(total_congestion_collected) as actual_revenue,
            SUM(trips_without_surcharge) * 9.0 as theoretical_revenue_gap
        FROM read_parquet('{zone_datamart_path}')
        WHERE after_congestion_start = 1
          AND zone_category IN ('entering_zone', 'exiting_zone')
        """
        
        raw_compliance_data = db_audit_engine.execute(compliance_sql).fetchone()
        
        audit_metrics = {
            'observed_volume': raw_compliance_data[0],
            'compliant_volume': raw_compliance_data[1],
            'leakage_volume': raw_compliance_data[2],
            'leakage_coeff': raw_compliance_data[3],
            'actual_revenue': raw_compliance_data[4],
            'revenue_gap': raw_compliance_data[5]
        }
        
        # Telemetry Output
        logger.info(f"\nAudit Findings (Post-{POLICY_EFFECTIVE_DATE}):")
        logger.info(f"  Cross-Border Volume: {audit_metrics['observed_volume']:,}")
        logger.info(f"  Compliant Transactions: {audit_metrics['compliant_volume']:,}")
        logger.info(f"  Detected Leakage (Gap): {audit_metrics['leakage_volume']:,}")
        logger.info(f"  System Leakage Factor: {audit_metrics['leakage_coeff']:.2f}%")
        logger.info(f"  Revenue Captured: ${audit_metrics['actual_revenue']:,.2f}")
        logger.info(f"  Estimated Revenue Gap: ${audit_metrics['revenue_gap']:,.2f}")
        
        # Persist Forensic Summary
        audit_summary_file = DATAMART_DIR / "forensic_compliance_summary.parquet"
        db_audit_engine.execute(f"""
        COPY (
            SELECT 
                {audit_metrics['observed_volume']} as gross_volume,
                {audit_metrics['compliant_volume']} as verified_volume,
                {audit_metrics['leakage_volume']} as leakage_volume,
                {audit_metrics['leakage_coeff']} as leakage_percent,
                {audit_metrics['actual_revenue']} as revenue_net,
                {audit_metrics['revenue_gap']} as revenue_loss_est
        )
        TO '{audit_summary_file}' (FORMAT PARQUET)
        """)
        
        logger.success(f"Forensic results exported to: {audit_summary_file.name}")
        
        db_audit_engine.close()
        return audit_metrics
        
    except Exception as audit_err:
        logger.error(f"Compliance audit failed: {audit_err}")
        return {}


def evaluate_inter_fleet_dynamics() -> dict:
    """
    Performs a comparative evaluation of Yellow and Green taxi performance 
    indicators across the pre-policy and post-policy quarters.
    """
    
    try:
        logger.info("\n" + "=" * 70)
        logger.info("EVALUATING INTER-FLEET DYNAMICS (Q1 '24 VS Q1 '25)")
        logger.info("=" * 70)
        
        db_analytics_engine = duckdb.connect()
        
        # Pre-Policy Baseline (Q1 2024)
        baseline_sql = f"""
        SELECT 
            CASE 
                WHEN source_file LIKE '%yellow%' THEN 'MEDALLION_YELLOW'
                ELSE 'MEDALLION_GREEN'
            END as fleet_identifier,
            COUNT(*) as trip_volume,
            AVG(fare) as mean_fare,
            SUM(total_amount) as gross_revenue
        FROM (
            SELECT *, current_filename() as source_file
            FROM read_parquet('{DATAMART_DIR.parent}/processed/purified/*purified*2024-0[1-3]*.parquet')
        )
        GROUP BY fleet_identifier
        """
        
        baseline_stats = db_analytics_engine.execute(baseline_sql).fetchall()
        
        # Post-Policy Horizon (Q1 2025)
        horizon_sql = f"""
        SELECT 
            CASE 
                WHEN source_file LIKE '%yellow%' THEN 'MEDALLION_YELLOW'
                ELSE 'MEDALLION_GREEN'
            END as fleet_identifier,
            COUNT(*) as trip_volume,
            AVG(fare) as mean_fare,
            SUM(total_amount) as gross_revenue
        FROM (
            SELECT *, current_filename() as source_file
            FROM read_parquet('{DATAMART_DIR.parent}/processed/purified/*purified*2025-0[1-3]*.parquet')
        )
        GROUP BY fleet_identifier
        """
        
        horizon_stats = db_analytics_engine.execute(horizon_sql).fetchall()
        
        # Comparative Synthesis
        logger.info("\nLongitudinal Performance Matrix:")
        logger.info("-" * 95)
        logger.info(f"{'Fleet ID':<18} {'Segment':<12} {'Vol (Δ %)':>15} {'Mean Fare':>15} {'Revenue':>20}")
        logger.info("-" * 95)
        
        dynamics_report_data = []
        
        for b_row in baseline_stats:
            fleet, b_vol, b_fare, b_rev = b_row
            
            # Match baseline with horizon
            for h_row in horizon_stats:
                if h_row[0] == fleet:
                    h_vol, h_fare, h_rev = h_row[1:]
                    
                    # Compute Elasticity %
                    vol_delta = ((h_vol - b_vol) / b_vol * 100) if b_vol > 0 else 0
                    fare_delta = ((h_fare - b_fare) / b_fare * 100) if b_fare > 0 else 0
                    rev_delta = ((h_rev - b_rev) / b_rev * 100) if b_rev > 0 else 0
                    
                    # Log Results
                    logger.info(f"{fleet:<18} {'Pre-Policy':<12} {b_vol:>15,} {b_fare:>15.2f} {b_rev:>20,.2f}")
                    logger.info(f"{fleet:<18} {'Post-Policy':<12} {h_vol:>15,} {h_fare:>15.2f} {h_rev:>20,.2f}")
                    logger.info(f"{fleet:<18} {'Δ Delta':<12} {vol_delta:>14.1f}% {fare_delta:>14.1f}% {rev_delta:>19.1f}%")
                    logger.info("-" * 95)
                    
                    dynamics_report_data.append({
                        'fleet': fleet,
                        'vol_pre': b_vol, 'vol_post': h_vol, 'vol_change': vol_delta,
                        'fare_pre': b_fare, 'fare_post': h_fare, 'fare_change': fare_delta,
                        'rev_pre': b_rev, 'rev_post': h_rev, 'rev_change': rev_delta
                    })
        
        # Export Behavioral Matrix
        behavioral_matrix_path = DATAMART_DIR / "fleet_behavioral_dynamics.parquet"
        
        if dynamics_report_data:
            import pandas as pd
            df_dynamics = pd.DataFrame(dynamics_report_data)
            db_analytics_engine.execute(f"COPY (SELECT * FROM df_dynamics) TO '{behavioral_matrix_path}' (FORMAT PARQUET)")
            logger.success(f"Behavioral matrix persisted at: {behavioral_matrix_path.name}")
        
        db_analytics_engine.close()
        return {'dynamics_data': dynamics_report_data}
        
    except Exception as dynamics_err:
        logger.error(f"Inter-fleet evaluation failed: {dynamics_err}")
        return {}


if __name__ == "__main__":
    """
    Main analytical pipeline entry point.
    """
    
    # 1. Compliance Audit
    compliance_report = conduct_operational_compliance_audit()
    
    # 2. Behavioral Evaluation
    dynamics_report = evaluate_inter_fleet_dynamics()
    
    # Validation Gate
    if compliance_report and dynamics_report:
        logger.success("Analytical engine finished all assigned tasks.")
        sys.exit(0)
    else:
        logger.error("Analytical engine encountered non-critical processing errors.")
        sys.exit(1)
