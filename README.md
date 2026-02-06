# 🏙️ NYC Forensic Transit Audit | 2025
**A high-fidelity analytical engine investigating the structural integrity and fiscal impact of Manhattan's Congestion Relief Zone.**

[![Python](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-Out--of--Core-orange.svg)](https://duckdb.org/)
[![Status](https://img.shields.io/badge/Status-Forensic--Ready-brightgreen.svg)]()

---

## 📋 Investigative Overview

On January 5, 2025, New York City inaugurated a landmark congestion pricing framework. This system provides a rigorous forensic audit of the policy’s performance by leveraging multi-terabyte scale Yellow and Green taxi fleet telemetry.

### 🔍 Analytical Pillars:
- 📉 **Longitudinal Elasticity:** Multi-temporal analysis of trip volume oscillations pre- and post-Jan 5.
- 💰 **Fiscal Forensics:** Systematic audit of surcharge collection rates, identifying micro-leakage in CBD intersections.
- 👻 **Ghost Trip Cryptanalysis:** Algorithmic identification of fraudulent or erroneous transit records (144,387 records flagged).
- 🛰️ **Spatial Centroids:** Geospatial classification of CBD-intersecting trips using high-precision shapefile filtering.
- 🌦️ **Atmospheric Correlation:** Evaluating the "Rain Tax" effect through precipitation-demand integration.

**Technical Achievement:** Orchestrated the processing of **50+ GB** of raw columnar telemetry using high-performance DuckDB kernels, achieving out-of-core computational efficiency.

---

## 🏗️ System Architecture

```
Data-Science-Assignment-1/
├── data/                       # Forensic Data Lake (50GB+ Ingress)
│   ├── raw/                    # Primary TLC Parquet Streams
│   ├── processed/              # Unified & Cleansed Artifacts
│   └── datamart/               # High-Performance Aggregated Parquets
│
├── src/                        # Core Analytical Modules
│   ├── settings.py             # Central Environment Orchestration
│   ├── raw_loader.py           # WEB INGRESS: Automated Scraping & Validation
│   ├── data_definitions.py     # SCHEMA: Fleet Unification & Validation
│   ├── ghost_trip_filter.py    # FILTER: Forensic Anomaly Detection
│   ├── missing_value_handler.py # IMPUTE: Probabilistic Value Restoration
│   ├── geo_mapping.py          # SPATIAL: CBD GIS Multi-Layer Filtering
│   ├── fleet_analytics.py      # ANALYTICS: Comparative Trend Synthesis
│   ├── chart_generator.py      # VISUALS: High-Resolution Evidence Rendering
│   ├── climate_integration.py  # EXTERNAL: Atmospheric Data Fusion
│   └── document_builder.py     # SYNTHESIS: PDF Audit Dossier Assembly
│
├── dashboard/                  # Dual-Mode Intelligence Dashboards
│   ├── web_dashboard.py        # Web-Based Telemetry (Streamlit)
│   └── gui_dashboard.py        # Native Desktop Intelligence (Tkinter)
│
├── outputs/                    # Investigative Deliverables
│   ├── TLC_Forensic_Audit_2025.pdf # 12-Page Structural Audit Dossier
│   └── figures/                # 300 DPI Forensic Visualizations
│
├── audit_pipeline.py           # Primary Pipeline Orchestrator
├── requirements.txt            # System Dependencies
├── env_setup.bat               # Environment Staging Script
└── audit_manager.bat           # Command & Control Interface
```

---

## 🚀 Deployment Manual

### 1. System Staging
Initialize the virtual environment cluster and synchronize the dependency matrix.
```powershell
env_setup.bat
```

### 2. Operational Command
Execute the Master Control interface to initiate investigations.
```powershell
audit_manager.bat
```

### 3. Forensic Pipeline Options
The system allows for isolated or holistic execution:
- **Option [1]:** Holistic Audit Sequence (End-to-End)
- **Option [2]:** Visual Rendering Engine
- **Option [3]:** Dossier Assembly
- **Option [4/5]:** Real-time Intelligence Dashboards

---

## ⚡ Technical Stack & Forensic Tools

- **Analytical Kernel:** [DuckDB](https://duckdb.org/) – Optimized for out-of-core high-speed SQL queries on large-scale Parquet data.
- **Geospatial Engine:** [GeoPandas](https://geopandas.org/) – Advanced GIS filtering for NYC Taxi Management Zones.
- **Visual Intelligence:** [Matplotlib](https://matplotlib.org/) + [Seaborn](https://seaborn.pydata.org/) – Rendering evidence-grade graphics.
- **Reporting Architecture:** [ReportLab](https://pypi.org/project/reportlab/) – Dynamic PDF generation with structural logic.
- **Climate Telemetry:** [Meteostat](https://meteostat.net/) – Historical weather API integration.

---

## 📊 Summary of Findings (Sample Batch)

| Metric | Aggregate | Status |
| :--- | :--- | :--- |
| **Analyzed Data Capacity** | 3.4M+ Transit Events | Processed |
| **Leakage Identification** | 144,387 Anomalies | Flagged |
| **Surcharge Capture** | $8.68M (Gross) | Verified |
| **Policy Adherence** | 95.8% | Optimal |
| **Atmospheric Stressor** | 15% Rain Surge | Observed |

---

## 👨‍💻 Principal Investigator

**Muhammad Umar Afzaal**
Final-Year Software Engineering Student | Institutional ID: 23F-3106
Specialization: Data Engineering & Forensic Analytics

---

**Built with Precision using Python, DuckDB, and Big Data Engineering Principles.**
