"""
INTERACTIVE WEB TELEMETRY DASHBOARD (Phase 10)
=============================================

This module executes a high-fidelity Streamlit interface for the 
interactive exploration of NYC Congestion Pricing Audit metrics.

Visual Framework:
- Dynamic KPI Telemetry: Real-time adherence and leakage tracking.
- Longitudinal Flux: Time-series analysis of transit oscillations.
- Financial Forensics: Revenue capture and toll elasticity.
- Spatial Centroids: Geographic distribution of CBD-intersections.

Operational Command:
    streamlit run dashboard/web_dashboard.py
"""

import streamlit as st
import pandas as pd
import duckdb
from pathlib import Path
import sys
from PIL import Image

# Workspace Configuration
sys.path.append(str(Path(__file__).parent.parent))

from src.settings import DATAMART_DIR, CHART_EXPORTS_DIR


# Institutional Configuration
st.set_page_config(
    page_title="NYC Forensic Transit Audit | 2025",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Forensic Aesthetic Injection (Premium Glassmorphism)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap');

    html, body, [class*="st-"] {
        font-family: 'Outfit', sans-serif;
    }

    .stApp {
        background: radial-gradient(circle at top right, #1e1b4b, #0f172a);
        color: #f1f5f9;
    }
    
    .main-header {
        font-size: 4rem;
        font-weight: 800;
        background: linear-gradient(120deg, #818cf8, #38bdf8);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 2.5rem 0 0.5rem 0;
        letter-spacing: -0.05em;
    }
    
    .sub-header {
        font-size: 1.4rem;
        color: #94a3b8;
        text-align: center;
        margin-bottom: 4rem;
        font-weight: 300;
        letter-spacing: 0.1em;
        text-transform: uppercase;
    }
    
    /* Premium Glassmorphic Cards */
    div[data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        padding: 2rem;
        border-radius: 1.5rem;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    }
    
    div[data-testid="stMetric"]:hover {
        transform: scale(1.02);
        background: rgba(255, 255, 255, 0.05);
        border-color: #6366f1;
        box-shadow: 0 0 20px rgba(99, 102, 241, 0.3);
    }
    
    /* Tabs Architecture */
    .stTabs [data-baseweb="tab-list"] {
        justify-content: center;
        background-color: transparent;
        border-bottom: 1px solid rgba(255,255,255,0.1);
        padding-bottom: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        font-size: 1.1rem;
        font-weight: 600;
        padding: 12px 24px;
        color: #64748b;
        background: transparent;
        transition: color 0.3s ease;
    }
    
    .stTabs [aria-selected="true"] {
        color: #818cf8 !important;
        border-bottom: 2px solid #818cf8 !important;
    }

    /* Narrative Blocks */
    .insight-card {
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid rgba(255, 255, 255, 0.05);
        padding: 2rem;
        border-radius: 1.2rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data
def fetch_longitudinal_audit_summaries():
    """Extracts high-level audit KPIs from the datamart layer."""
    try:
        db_conn = duckdb.connect()
        spatial_artifact = DATAMART_DIR / "trips_by_zone_category.parquet"
        
        if spatial_artifact.exists():
            kpi_metrics = db_conn.execute(f"""
                SELECT 
                    SUM(trip_count) as record_volume,
                    SUM(total_congestion_collected) as gross_revenue,
                    SUM(trips_with_surcharge) as compliant_records,
                    SUM(trips_without_surcharge) as leakage_records,
                    AVG(avg_fare) as mean_fare
                FROM read_parquet('{spatial_artifact}')
                WHERE after_congestion_start = 1
            """).fetchone()
            
            volume = kpi_metrics[0] or 0
            revenue = kpi_metrics[1] or 0
            compliant = kpi_metrics[2] or 0
            leakage = kpi_metrics[3] or 0
            
            adherence_ratio = (compliant / (compliant + leakage) * 100) if (compliant + leakage) > 0 else 0
            leakage_coefficient = 100 - adherence_ratio
            fiscal_leakage = leakage * 2.50  # Theoretical missed collection
            
            return {
                'volume': volume,
                'revenue': revenue,
                'adherence': adherence_ratio,
                'leakage': leakage_coefficient,
                'fiscal_gap': fiscal_leakage
            }
        
    except Exception as telemetry_err:
        st.error(f"Telemetry Acquisition Error: {telemetry_err}")
    
    return None


@st.cache_data
def fetch_spatial_distribution_metrics():
    """Extracts categorical spatial distribution for the datamart."""
    try:
        db_conn = duckdb.connect()
        spatial_artifact = DATAMART_DIR / "trips_by_zone_category.parquet"
        
        if spatial_artifact.exists():
            distribution_df = db_conn.execute(f"""
                SELECT 
                    zone_category,
                    after_congestion_start,
                    SUM(trip_count) as volume
                FROM read_parquet('{spatial_artifact}')
                GROUP BY 1, 2
                ORDER BY 1, 2
            """).df()
            return distribution_df
            
    except Exception as spatial_err:
        st.error(f"Spatial Telemetry Error: {spatial_err}")
    
    return None


def orchestrate_web_telemetry_dashboard():
    """Primary entry point for the web-based forensic interface."""
    
    # 1. Header Synthesis
    st.markdown('<div class="main-header">FORENSIC TRANSIT AUDIT</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">NYC Congestion Pricing Intelligence Engine | 2025</div>', unsafe_allow_html=True)
    
    # 2. Control Perimeter (Sidebar)
    with st.sidebar:
        st.markdown('<h2 style="text-align: center; color: #818cf8;">AUDIT COMMAND</h2>', unsafe_allow_html=True)
        st.markdown("---")
        
        selected_phase = st.radio("Pipeline Telemetry:", ["Overview", "Temporal Analysis", "Fiscal Flux", "Geospatial Logs"], key="sidebar_navigation")
        
        st.markdown("---")
        st.info("Source: NYC TLC Structural Data")
        st.success("Kernel: DuckDB High-Performance")
        
        st.markdown("---")
        st.markdown('<p style="font-size: 0.85rem; color: #94a3b8; text-align: center;"><b>Lead Forensic Architect:</b><br>Muhammad Umar Afzaal<br>Roll: 23F-3106</p>', unsafe_allow_html=True)
    
    # 3. Data Integration
    audit_kpis = fetch_longitudinal_audit_summaries()
    
    # 4. Interface Orchestration
    tab_overview, tab_temporal, tab_fiscal, tab_geospatial = st.tabs([
        "💎 Executive Synthesis", "🌀 Temporal Flux", "⚡ Fiscal Forensics", "🛰️ Spatial Invariance"
    ])
    
    # --- TAB: EXECUTIVE SYNTHESIS ---
    with tab_overview:
        if audit_kpis:
            # KPI Matrix
            met1, met2, met3, met4 = st.columns(4)
            met1.metric("Analyzed Capacity", f"{audit_kpis['volume']:,}", "Records")
            met2.metric("Gross Revenue", f"${audit_kpis['revenue']:,.2f}", f"${audit_kpis['revenue'] / 1e6:.1f}M")
            met3.metric("Policy Adherence", f"{audit_kpis['adherence']:.2f}%", "Verified")
            met4.metric("Revenue Leakage", f"{audit_kpis['leakage']:.2f}%", f"-${audit_kpis['fiscal_gap'] / 1e3:.1f}K", delta_color="inverse")
            
            st.markdown('<div style="margin: 3rem 0;"></div>', unsafe_allow_html=True)
            
            # Narrative Forensics
            col_insight, col_strategy = st.columns(2)
            with col_insight:
                st.markdown(f"""
                <div class="insight-card">
                    <h3 style="color: #38bdf8;">Forensic Insights</h3>
                    <p style="color: #cbd5e1;">The system has processed <b>{audit_kpis['volume']:,}</b> discrete transit events. Adherence levels remain robust at <b>{audit_kpis['adherence']:.2f}%</b>, though micro-leakage in the outer-edge CBD hubs contributes to a calculated fiscal gap of ${audit_kpis['fiscal_gap']:,.2f}.</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col_strategy:
                st.markdown("""
                <div class="insight-card" style="border-left: 4px solid #10b981;">
                    <h3 style="color: #10b981;">Strategic Commands</h3>
                    <ul style="color: #cbd5e1;">
                        <li>Deploy enhanced geofencing at outer-perimeter zones.</li>
                        <li>Automate surcharge validation within the dispatch kernel.</li>
                        <li>Synchronize toll windows with real-time thermal congestion cycles.</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.warning("Audit Telemetry Offline: Please initialize the analytical pipeline.")

    # --- TAB: TEMPORAL FLUX ---
    with tab_temporal:
        st.markdown("### Longitudinal Transit Oscillations")
        time_series_path = CHART_EXPORTS_DIR / "temporal_volume_dynamics.png"
        
        if time_series_path.exists():
            st.image(Image.open(time_series_path), width="stretch")
            st.info("The visualization illustrates the demand elasticity post-Jan 5 implementation.")
        else:
            st.error("Visual Artifact Missing: Execute `chart_generator.py` to synthesize plots.")

    # --- TAB: FISCAL FORENSICS ---
    with tab_fiscal:
        st.markdown("### Congestion Revenue Capture Dynamics")
        revenue_plot_path = CHART_EXPORTS_DIR / "fiscal_trajectory_mapping.png"
        
        if revenue_plot_path.exists():
            st.image(Image.open(revenue_plot_path), width="stretch")
            st.info("Correlation between surcharge compliance and temporal demand cycles.")
        else:
            st.error("Visual Artifact Missing.")

    # --- TAB: SPATIAL INVARIANCE ---
    with tab_geospatial:
        st.markdown("### CBD Intersection - Spatial Cluster Analysis")
        spatial_plot_path = CHART_EXPORTS_DIR / "spatial_load_distribution.png"
        
        if spatial_plot_path.exists():
            st.image(Image.open(spatial_plot_path), width="stretch")
            
            distribution_metrics = fetch_spatial_distribution_metrics()
            if distribution_metrics is not None:
                st.markdown("#### Tabular Distribution Summary")
                # Structural transformation for pivot view
                distribution_metrics['Phase'] = distribution_metrics['after_congestion_start'].map({0: 'Baseline', 1: 'Policy'})
                final_pivot = distribution_metrics.pivot_table(index='zone_category', columns='Phase', values='volume', fill_value=0)
                st.dataframe(final_pivot, width="stretch")
        leakage_plot_path = CHART_EXPORTS_DIR / "compliance_leakage_forensics.png"
        
        if leakage_plot_path.exists():
            st.markdown("---")
            st.markdown("### Forensic Compliance & Leakage Analysis")
            st.image(Image.open(leakage_plot_path), width="stretch")
        else:
            st.error("Visual Artifact Missing.")

    # 5. Footer Persistence
    st.markdown('<div style="margin: 5rem 0;"></div>', unsafe_allow_html=True)
    st.markdown("""
    <div style='text-align: center; color: #475569; padding: 2rem; border-top: 1px solid rgba(255,255,255,0.05);'>
        <p style="font-weight: 600; font-size: 1.1rem; color: #94a3b8;">NYC TRANSIT FORENSICS | INSTITUTIONAL AUDIT ENGINE</p>
        <p>© 2025 Forensic Architecture by Muhammad Umar Afzaal</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    orchestrate_web_telemetry_dashboard()
