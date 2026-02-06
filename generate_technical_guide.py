"""
TECHNICAL ARCHITECTURE GUIDE GENERATOR
======================================

This standalone script generates a comprehensive technical PDF guide explaining 
the "NYC Congestion Pricing Forensic Audit 2026" system architecture.

It details:
1. Pipeline Architecture
2. Folder Structure & Functionality
3. Data Ingestion & EtL Flow
4. Visualization Logic
5. Detailed Function Walkthroughs

Usage:
    python generate_technical_guide.py
"""

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak, 
    Table, TableStyle, Image as RLImage, ListFlowable, ListItem
)
from reportlab.lib import colors
from datetime import datetime
from pathlib import Path
import sys

# Define Output Path
OUTPUT_PDF = Path("Project_Architecture_Guide_2026.pdf")

def generate_guide():
    print(f"Generating Technical Guide: {OUTPUT_PDF.absolute()}")
    
    doc = SimpleDocTemplate(
        str(OUTPUT_PDF),
        pagesize=letter,
        rightMargin=72, leftMargin=72,
        topMargin=72, bottomMargin=40,
    )
    
    elements = []
    styles = getSampleStyleSheet()
    
    # Custom Styles
    TITLE_STYLE = ParagraphStyle(
        'Title', parent=styles['Heading1'], fontSize=24, 
        alignment=TA_CENTER, spaceAfter=20, textColor=colors.HexColor('#1E1B4B')
    )
    HEADER_STYLE = ParagraphStyle(
        'Header', parent=styles['Heading2'], fontSize=16, 
        spaceBefore=15, spaceAfter=10, textColor=colors.HexColor('#1E293B')
    )
    SUBHEADER_STYLE = ParagraphStyle(
        'SubHeader', parent=styles['Heading3'], fontSize=12, 
        spaceBefore=10, spaceAfter=5, textColor=colors.HexColor('#334155')
    )
    BODY_STYLE = ParagraphStyle(
        'Body', parent=styles['BodyText'], fontSize=10, 
        leading=14, alignment=TA_JUSTIFY, spaceAfter=10
    )
    CODE_STYLE = ParagraphStyle(
        'Code', parent=styles['Code'], fontSize=9, 
        leading=12, fontName='Courier', backColor=colors.whitesmoke,
        borderPadding=5, spaceAfter=10
    )

    # --- TITLE PAGE ---
    elements.append(Spacer(1, 2*inch))
    elements.append(Paragraph("NYC Forensic Transit Audit System", TITLE_STYLE))
    elements.append(Paragraph("Technical Architecture & Operational Guide", 
        ParagraphStyle('Subtitle', parent=styles['Normal'], fontSize=14, alignment=TA_CENTER)))
    elements.append(Spacer(1, 1*inch))
    elements.append(Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y')}", 
        ParagraphStyle('Date', parent=styles['Normal'], fontSize=10, alignment=TA_CENTER)))
    elements.append(Paragraph("Author: Muhammad Umar Afzaal", 
        ParagraphStyle('Author', parent=styles['Normal'], fontSize=10, alignment=TA_CENTER)))
    elements.append(PageBreak())

    # --- 1. SYSTEM OVERVIEW ---
    elements.append(Paragraph("1. System Overview", HEADER_STYLE))
    elements.append(Paragraph(
        "This project implements a forensic data audit pipeline designed to analyze New York City "
        "taxi data before and after the 2025 Congestion Pricing implementation. It leverages "
        "DuckDB for out-of-core processing (handling large datasets without memory crashes) "
        "and provides both interactive (Streamlit/Tkinter) and static (PDF) reporting layers.",
        BODY_STYLE
    ))

    # --- 2. FOLDER STRUCTURE ---
    elements.append(Paragraph("2. Project Directory Structure", HEADER_STYLE))
    
    folder_data = [
        ["Directory/File", "Description & Responsibility"],
        ["audit_manager.bat", "Master Control Script (CLI Entry Point)."],
        ["audit_pipeline.py", "Main Python Orchestrator. Coordinates phases 1-8."],
        ["src/", "Core Source Code Modules."],
        ["  ├── raw_loader.py", "Data Ingestion: Downloads Parquet files from NYC TLC."],
        ["  ├── data_definitions.py", "Schema Unification: Maps Yellow/Green taxonomies."],
        ["  ├── ghost_trip_filter.py", "Data Refinery: Removes anomalies (speed > 65mph)."],
        ["  ├── missing_value_handler.py", "Imputation: SI-Model for missing Dec 2025 data."],
        ["  ├── geo_mapping.py", "Spatial Logic: Defines CBD zones & categorizes trips."],
        ["  ├── fleet_analytics.py", "Analytics Engine: Calculates revenue, leakage, metrics."],
        ["  ├── chart_generator.py", "Visualization: Generates Matplotlib/Seaborn plots."],
        ["  └── document_builder.py", "Reporting: Compiles the final PDF dossier."],
        ["dashboard/", "UI Application Code."],
        ["  ├── web_dashboard.py", "Streamlit Web App (Interactive Telemetry)."],
        ["  └── gui_dashboard.py", "Tkinter Desktop App (Native Interface)."],
        ["data/", "Data Warehouse (Raw, Processed, Datamarts)"],
        ["outputs/", "Generated Artifacts (PDFs, Logs, Figures)"]
    ]
    
    t = Table(folder_data, colWidths=[2.5*inch, 4.5*inch])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#E2E8F0')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.HexColor('#1E293B')),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('GRID', (0,0), (-1,-1), 0.25, colors.grey),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 6),
        ('RIGHTPADDING', (0,0), (-1,-1), 6),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 0.2*inch))

    # --- 3. ETL PIPELINE DETAIL ---
    elements.append(Paragraph("3. ETL Pipeline Architecture", HEADER_STYLE))
    
    # Phase 1
    elements.append(Paragraph("Phase 1: Ingestion (raw_loader.py)", SUBHEADER_STYLE))
    elements.append(Paragraph(
        "<b>Function:</b> `execute_full_data_harvest()`<br/>"
        "<b>Logic:</b> Connects to the NYC TLC S3 bucket. It iterates through the target months (Jan-Mar 2024 & 2025). "
        "It uses `requests` to download the Parquet files and verifies integrity using file size checks.",
        BODY_STYLE
    ))

    # Phase 2
    elements.append(Paragraph("Phase 2: Schema Alignmnent (data_definitions.py)", SUBHEADER_STYLE))
    elements.append(Paragraph(
        "<b>Function:</b> `orchestrate_fleet_schema_alignment()`<br/>"
        "<b>Logic:</b> Yellow and Green taxis have different column names (e.g., 'tpep_pickup_datetime' vs 'lpep_pickup_datetime'). "
        "This module maps them to a single canonical schema: `[pickup_time, dropoff_time, pickup_loc, ...]`. "
        "It uses DuckDB to perform this transformation efficiently on disk.",
        BODY_STYLE
    ))

    # Phase 3
    elements.append(Paragraph("Phase 3: Cleaning (ghost_trip_filter.py)", SUBHEADER_STYLE))
    elements.append(Paragraph(
        "<b>Function:</b> `process_refinery_batch()`<br/>"
        "<b>Logic:</b> Removes 'Ghost Trips'. Applies detection heuristics:<br/>"
        "- Velocity > 65 MPH (Impossible in NYC)<br/>"
        "- Duration < 60s with Fare > $20 (Suspicious)<br/>"
        "- Negative Fares (System errors)<br/>"
        "Clean data is saved to `data/processed/purified/`.",
        BODY_STYLE
    ))

    # Phase 4
    elements.append(Paragraph("Phase 4: Missing Data (missing_value_handler.py)", SUBHEADER_STYLE))
    elements.append(Paragraph(
        "<b>Function:</b> `run_comprehensive_data_recovery()`<br/>"
        "<b>Logic:</b> Simulates the future (Dec 2025) if data is missing. "
        "It uses an SI-Model (Synthesis-Imputation) which takes 70% of Dec 2024 patterns and blends "
        "them with a 30% growth trend from early 2025 to create a realistic synthetic dataset.",
        BODY_STYLE
    ))

    # --- 4. ANALYTICS & VISUALIZATION ---
    elements.append(Paragraph("4. Analytics & Visualization", HEADER_STYLE))
    
    elements.append(Paragraph("Geospatial Logic (geo_mapping.py)", SUBHEADER_STYLE))
    elements.append(Paragraph(
        "Identifies trips entering/exiting the Manhattan CBD. "
        "It queries the taxi zone shapefiles. If a pickup is Outside CBD and Dropoff is Inside CBD, "
        "it tags the trip as 'entering_zone' (Liable for Toll).",
        BODY_STYLE
    ))

    elements.append(Paragraph("Visualization Engine (chart_generator.py)", SUBHEADER_STYLE))
    elements.append(Paragraph(
        "Generates the static PNGs found in `outputs/figures/`. "
        "Uses Matplotlib and Seaborn. It reads the aggregated metrics from DuckDB and plots:<br/>"
        "- <b>Time Series:</b> Daily trip volume trends.<br/>"
        "- <b>Fiscal Trajectory:</b> Revenue collected vs Leakage.<br/>"
        "- <b>Spatial Load:</b> Which zones have high traffic.",
        BODY_STYLE
    ))

    # --- 5. EXECUTION & FLOW ---
    elements.append(Paragraph("5. Execution Flow Execution", HEADER_STYLE))
    elements.append(Paragraph(
        "When you run `audit_manager.bat` -> Option [1]:", 
        BODY_STYLE
    ))
    
    flow_steps = [
        ListItem(Paragraph("<b>Start:</b> `audit_pipeline.py` initializes logging.", BODY_STYLE)),
        ListItem(Paragraph("<b>Step 1:</b> Downloads Raw Data (`raw_loader`).", BODY_STYLE)),
        ListItem(Paragraph("<b>Step 2:</b> Unifies Schemas (`data_definitions`).", BODY_STYLE)),
        ListItem(Paragraph("<b>Step 3:</b> Filters Ghost Trips (`ghost_trip_filter`).", BODY_STYLE)),
        ListItem(Paragraph("<b>Step 4:</b> Imputes Missing Dec 2025 (`missing_value_handler`).", BODY_STYLE)),
        ListItem(Paragraph("<b>Step 5:</b> Maps Spatial Zones (`geo_mapping`).", BODY_STYLE)),
        ListItem(Paragraph("<b>Step 6:</b> Calculates Metrics (`fleet_analytics`).", BODY_STYLE)),
        ListItem(Paragraph("<b>Step 7:</b> Generates Charts (`chart_generator`).", BODY_STYLE)),
        ListItem(Paragraph("<b>Step 8:</b> Compiles PDF (`document_builder`).", BODY_STYLE)),
        ListItem(Paragraph("<b>End:</b> Pipeline finishes. Output available in `outputs/`.", BODY_STYLE)),
    ]
    elements.append(ListFlowable(flow_steps, bulletType='bullet', start='circle'))

    # Final Build
    doc.build(elements)
    print("PDF Generation Successful.")

if __name__ == "__main__":
    generate_guide()
