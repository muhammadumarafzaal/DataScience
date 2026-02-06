"""
HIGH-FIDELITY ANALYTICAL VISUALIZATION (Phase 6)
===============================================

This module orchestrates the generation of static analytical artifacts 
(charts and graphs) designed for inclusion in the final audit report.

Visual Standards:
- High-resolution rendering (300 DPI).
- Premium dark-glass aesthetic utilizing the system color palette.
- Descriptive titling and statistical annotations.

Core Visualization Suite:
1. Temporal Volume Analysis: Longitudinal study of trip frequencies.
2. Fiscal Trajectory Mapping: Cumulative and daily revenue capture.
3. Spatial Load Distribution: Trip density across zone classifications.
4. Compliance Leakage Forensics: Visualizing toll non-compliance rates.
"""

import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from loguru import logger
import sys
import pandas as pd
import numpy as np

# Integration with system settings
from src.settings import (
    DATAMART_DIR, CHART_EXPORTS_DIR, AUDIT_LOGS_DIR,
    OPTIMAL_FIGURE_WD, OPTIMAL_FIGURE_HT
)

# Operational Logging Configuration
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
logger.add(AUDIT_LOGS_DIR / "visual_generation.log", rotation="10 MB")

# Plotting Environment Initialization
plt.style.use('bmh')
sns.set_context("talk")

# Premium Audit Color Schema
AUDIT_COLOR_SCHEMA = {
    'primary': '#6366F1',    # Indigo 600
    'secondary': '#10B981',  # Emerald 500
    'accent': '#F59E0B',     # Amber 500
    'critical': '#EF4444',   # Rose 500
    'canvas_bg': '#F9FAFB',  # Gray 50
    'text_main': '#1F2937',  # Gray 800
    'grid_soft': '#F3F4F6'   # Gray 100
}

# Rendering Constants
OUTPUT_RESOLUTION_DPI = 300
DEFAULT_CANVAS_SIZE = (14, 8)


def generate_temporal_volume_analysis() -> None:
    """
    Constructs a longitudinal line plot illustrating trip volume 
    fluctuations across the audit period.
    """
    
    try:
        logger.info("Visualizing temporal volume trends...")
        
        db_conn = duckdb.connect()
        
        # Aggregate longitudinal trip volumes
        telemetry_query = f"""
        SELECT 
            trip_date,
            SUM(trip_count) as volume_sum
        FROM read_parquet('{DATAMART_DIR}/trips_by_zone_category.parquet')
        GROUP BY trip_date
        ORDER BY trip_date
        """
        
        volume_df = db_conn.execute(telemetry_query).df()
        
        # Initialize Canvas
        canvas, axes = plt.subplots(figsize=DEFAULT_CANVAS_SIZE, dpi=OUTPUT_RESOLUTION_DPI, facecolor=AUDIT_COLOR_SCHEMA['canvas_bg'])
        axes.set_facecolor(AUDIT_COLOR_SCHEMA['canvas_bg'])
        
        # Render Volume Trajectory
        axes.plot(volume_df['trip_date'], volume_df['volume_sum'], 
                 color=AUDIT_COLOR_SCHEMA['primary'], linewidth=3.5, marker='o', 
                 markersize=8, markerfacecolor='white', markeredgewidth=2.5, 
                 label='Observed Volume', zorder=3)
        
        # Shaded Probability Density
        axes.fill_between(volume_df['trip_date'], volume_df['volume_sum'], 
                         color=AUDIT_COLOR_SCHEMA['primary'], alpha=0.15, zorder=2)
        
        # Aesthetic Refinement
        axes.set_title('NYC Transport Network: Longitudinal Volume Analysis', fontsize=22, fontweight='bold', pad=35, color=AUDIT_COLOR_SCHEMA['text_main'])
        axes.set_xlabel('Audit Calendar Timeline', fontsize=16, labelpad=20)
        axes.set_ylabel('Total Diary Volume', fontsize=16, labelpad=20)
        
        axes.grid(True, linestyle=':', alpha=0.7, color=AUDIT_COLOR_SCHEMA['grid_soft'], zorder=1)
        
        # Axis Cleaning
        sns.despine(ax=axes, top=True, right=True)
        
        # Formatting Scales
        axes.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        plt.xticks(rotation=35, ha='right', fontsize=12)
        
        # Annotate Policy Trigger Point
        policy_trigger = pd.to_datetime('2025-01-05')
        if policy_trigger in pd.to_datetime(volume_df['trip_date']).values:
            axes.axvline(policy_trigger, color=AUDIT_COLOR_SCHEMA['critical'], linestyle='--', linewidth=2.5, alpha=0.9)
            axes.text(policy_trigger, axes.get_ylim()[1]*0.85, ' POLICY TRIGGER', 
                     color=AUDIT_COLOR_SCHEMA['critical'], fontweight='bold', fontsize=12, rotation=0)

        # Legend Overlay
        axes.legend(frameon=True, fontsize=13, loc='upper right', shadow=True)
        
        # Export Artifact
        save_dest = CHART_EXPORTS_DIR / "temporal_volume_dynamics.png"
        plt.savefig(save_dest, dpi=OUTPUT_RESOLUTION_DPI, bbox_inches='tight')
        plt.close()
        
        logger.success(f"Temporal artifact exported: {save_dest.name}")
        db_conn.close()
        
    except Exception as vis_err:
        logger.error(f"Temporal visualization failure: {vis_err}")


def generate_fiscal_trajectory_mapping() -> None:
    """
    Renders a dual-axis visualization mapping daily revenue capture 
    against cumulative fiscal benchmarks.
    """
    
    try:
        logger.info("Mapping fiscal trajectory...")
        
        db_conn = duckdb.connect()
        
        # Retrieve daily fiscal snapshots
        fiscal_query = f"""
        SELECT 
            trip_date,
            SUM(total_congestion_collected) as daily_yield
        FROM read_parquet('{DATAMART_DIR}/trips_by_zone_category.parquet')
        WHERE after_congestion_start = 1
        GROUP BY trip_date
        ORDER BY trip_date
        """
        
        fiscal_df = db_conn.execute(fiscal_query).df()
        fiscal_df['accumulated_yield'] = fiscal_df['daily_yield'].cumsum()
        
        # Initialize Canvas
        canvas, axes_prime = plt.subplots(figsize=DEFAULT_CANVAS_SIZE, dpi=OUTPUT_RESOLUTION_DPI, facecolor=AUDIT_COLOR_SCHEMA['canvas_bg'])
        axes_prime.set_facecolor(AUDIT_COLOR_SCHEMA['canvas_bg'])
        
        # Render Periodic Yield (Bars)
        axes_prime.bar(fiscal_df['trip_date'], fiscal_df['daily_yield'], 
                      color=AUDIT_COLOR_SCHEMA['secondary'], alpha=0.55, label='Daily Revenue Capture', zorder=2)
        
        axes_prime.set_xlabel('Fiscal Horizon', fontsize=16, labelpad=20)
        axes_prime.set_ylabel('Periodic Yield ($)', fontsize=16, color=AUDIT_COLOR_SCHEMA['secondary'])
        axes_prime.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${int(x):,}'))
        
        # Render Cumulative Trajectory (Secondary Axis)
        axes_sec = axes_prime.twinx()
        axes_sec.plot(fiscal_df['trip_date'], fiscal_df['accumulated_yield'], 
                     color=AUDIT_COLOR_SCHEMA['critical'], linewidth=4.5, marker='o', markersize=9, 
                     markerfacecolor='white', markeredgewidth=3, label='Cumulative Revenue Growth', zorder=4)
        
        axes_sec.set_ylabel('Aggregated Revenue ($)', fontsize=16, color=AUDIT_COLOR_SCHEMA['critical'])
        axes_sec.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${int(x):,}'))
        
        # Aesthetic Synchronicity
        axes_prime.set_title('NYC Transport Audit: Fiscal Capture Velocity', fontsize=22, fontweight='bold', pad=35)
        axes_prime.grid(True, linestyle=':', alpha=0.3, color=AUDIT_COLOR_SCHEMA['grid_soft'], zorder=1)
        plt.setp(axes_prime.get_xticklabels(), rotation=35, ha='right', fontsize=12)
        
        # Combined Legend Configuration
        prime_handles, prime_labels = axes_prime.get_legend_handles_labels()
        sec_handles, sec_labels = axes_sec.get_legend_handles_labels()
        axes_prime.legend(prime_handles + sec_handles, prime_labels + sec_labels, 
                         loc='upper left', frameon=True, shadow=True, fontsize=13)
        
        # Export Artifact
        save_dest = CHART_EXPORTS_DIR / "fiscal_trajectory_mapping.png"
        plt.savefig(save_dest, dpi=OUTPUT_RESOLUTION_DPI, bbox_inches='tight')
        plt.close()
        
        logger.success(f"Fiscal artifact exported: {save_dest.name}")
        db_conn.close()
        
    except Exception as vis_err:
        logger.error(f"Fiscal visualization failure: {vis_err}")


def generate_spatial_load_distribution() -> None:
    """
    Constructs a comparative categorical bar chart analyzing trip distribution 
    shifts across geographical zone classifications.
    """
    
    try:
        logger.info("Visualizing spatial load distribution...")
        
        db_conn = duckdb.connect()
        
        # Aggregation across zone categories and policy phases
        spatial_query = f"""
        SELECT 
            zone_category,
            CASE WHEN after_congestion_start = 1 THEN 'PHASE_POST_POLICY' ELSE 'PHASE_BASELINE' END as policy_phase,
            SUM(trip_count) as volume_metric
        FROM read_parquet('{DATAMART_DIR}/trips_by_zone_category.parquet')
        GROUP BY zone_category, policy_phase
        ORDER BY zone_category, policy_phase
        """
        
        spatial_df = db_conn.execute(spatial_query).df()
        
        # Initialize Canvas
        canvas, axes = plt.subplots(figsize=DEFAULT_CANVAS_SIZE, dpi=OUTPUT_RESOLUTION_DPI, facecolor=AUDIT_COLOR_SCHEMA['canvas_bg'])
        axes.set_facecolor(AUDIT_COLOR_SCHEMA['canvas_bg'])
        
        # Dimensional Configuration
        categories = spatial_df['zone_category'].unique()
        indices = np.arange(len(categories))
        bar_width = 0.4
        
        baseline_slice = spatial_df[spatial_df['policy_phase'] == 'PHASE_BASELINE'].set_index('zone_category')['volume_metric']
        post_policy_slice = spatial_df[spatial_df['policy_phase'] == 'PHASE_POST_POLICY'].set_index('zone_category')['volume_metric']
        
        # Render Comparative Bars
        axes.bar(indices - bar_width/2, [baseline_slice.get(c, 0) for c in categories], 
                bar_width, label='Baseline Period', color=AUDIT_COLOR_SCHEMA['accent'], alpha=0.75, edgecolor='black', linewidth=0.8)
        axes.bar(indices + bar_width/2, [post_policy_slice.get(c, 0) for c in categories], 
                bar_width, label='Post-Implementation', color=AUDIT_COLOR_SCHEMA['primary'], alpha=0.85, edgecolor='black', linewidth=0.8)
        
        # Aesthetic Refinement
        axes.set_title('Spatial Dynamics: Regional Volume Distribution Delta', fontsize=22, fontweight='bold', pad=35)
        axes.set_xlabel('Categorical Zone Classification', fontsize=16, labelpad=20)
        axes.set_ylabel('Aggregate Trip Count', fontsize=16, labelpad=20)
        
        axes.set_xticks(indices)
        axes.set_xticklabels(categories, rotation=35, ha='right', fontsize=12)
        axes.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        
        axes.grid(True, axis='y', linestyle=':', alpha=0.5, color=AUDIT_COLOR_SCHEMA['grid_soft'])
        sns.despine(ax=axes)
        
        axes.legend(frameon=True, shadow=True, fontsize=13)
        
        # Export Artifact
        save_dest = CHART_EXPORTS_DIR / "spatial_load_distribution.png"
        plt.savefig(save_dest, dpi=OUTPUT_RESOLUTION_DPI, bbox_inches='tight')
        plt.close()
        
        logger.success(f"Spatial artifact exported: {save_dest.name}")
        db_conn.close()
        
    except Exception as vis_err:
        logger.error(f"Spatial visualization failure: {vis_err}")


def generate_compliance_leakage_forensics() -> None:
    """
    Renders a forensic stacked area visualization depicting the variance 
    between compliant revenue capture and identified linkage (leakage).
    """
    
    try:
        logger.info("Visualizing compliance leakage forensics...")
        
        db_conn = duckdb.connect()
        
        # Temporal compliance tracking
        leakage_query = f"""
        SELECT 
            trip_date,
            SUM(trips_with_surcharge) as compliant_vol,
            SUM(trips_without_surcharge) as leakage_vol
        FROM read_parquet('{DATAMART_DIR}/trips_by_zone_category.parquet')
        WHERE after_congestion_start = 1
        GROUP BY trip_date
        ORDER BY trip_date
        """
        
        leakage_df = db_conn.execute(leakage_query).df()
        
        # Initialize Canvas
        canvas, axes = plt.subplots(figsize=DEFAULT_CANVAS_SIZE, dpi=OUTPUT_RESOLUTION_DPI, facecolor=AUDIT_COLOR_SCHEMA['canvas_bg'])
        axes.set_facecolor(AUDIT_COLOR_SCHEMA['canvas_bg'])
        
        # Stacked Behavioral Layers
        axes.fill_between(leakage_df['trip_date'], 0, leakage_df['compliant_vol'], 
                         color=AUDIT_COLOR_SCHEMA['secondary'], alpha=0.5, label='Compliant Transactions', zorder=2)
        axes.fill_between(leakage_df['trip_date'], leakage_df['compliant_vol'], 
                         leakage_df['compliant_vol'] + leakage_df['leakage_vol'], 
                         color=AUDIT_COLOR_SCHEMA['critical'], alpha=0.5, label='Revenue Leakage (Non-Compliant)', zorder=2)
        
        # Perimeter Definition
        axes.plot(leakage_df['trip_date'], leakage_df['compliant_vol'], color=AUDIT_COLOR_SCHEMA['secondary'], linewidth=2.5, alpha=0.9, zorder=3)
        axes.plot(leakage_df['trip_date'], leakage_df['compliant_vol'] + leakage_df['leakage_vol'], color=AUDIT_COLOR_SCHEMA['critical'], linewidth=2.5, alpha=0.9, zorder=3)
        
        # Aesthetic Refinement
        axes.set_title('Forensic Audit: Transaction Compliance & System Leakage', fontsize=22, fontweight='bold', pad=35)
        axes.set_xlabel('Operational Horizon', fontsize=16, labelpad=20)
        axes.set_ylabel('Transaction Volume', fontsize=16, labelpad=20)
        
        axes.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
        plt.xticks(rotation=35, ha='right', fontsize=12)
        
        axes.grid(True, linestyle=':', alpha=0.5, color=AUDIT_COLOR_SCHEMA['grid_soft'], zorder=1)
        sns.despine(ax=axes)
        
        axes.legend(loc='upper left', frameon=True, shadow=True, fontsize=13)
        
        # Export Artifact
        save_dest = CHART_EXPORTS_DIR / "compliance_leakage_forensics.png"
        plt.savefig(save_dest, dpi=OUTPUT_RESOLUTION_DPI, bbox_inches='tight')
        plt.close()
        
        logger.success(f"Forensic artifact exported: {save_dest.name}")
        db_conn.close()
        
    except Exception as vis_err:
        logger.error(f"Forensic visualization failure: {vis_err}")


def orchestrate_visual_audit_generation() -> None:
    """
    Main entry point for generating the complete architectural suite of 
    visual audit artifacts.
    """
    
    logger.info("=" * 70)
    logger.info("SYSTEM INITIALIZATION: BATCH VISUAL AUDIT GENERATION")
    logger.info("=" * 70)
    
    # Ensure infrastructure exists
    CHART_EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Batch Execution
    generate_temporal_volume_analysis()
    generate_fiscal_trajectory_mapping()
    generate_spatial_load_distribution()
    generate_compliance_leakage_forensics()
    
    logger.success("Visual audit generation finalized.")


if __name__ == "__main__":
    """
    Direct invocation for standalone visual collateral production.
    """
    orchestrate_visual_audit_generation()
    sys.exit(0)
