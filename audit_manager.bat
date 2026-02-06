@echo off
REM ============================================================================
REM NYC Forensic Transit Audit 2025 - Command ^& Control Center
REM ============================================================================
REM Lead Architect: Muhammad Umar Afzaal (23F-3106)
REM ============================================================================

setlocal enabledelayedexpansion

:MENU
cls
echo.
echo ============================================================================
echo           NYC FORENSIC TRANSIT AUDIT 2025 - COMMAND CENTER
echo ============================================================================
echo.
echo  [1] Initialize Holistic Audit Pipeline (Sequential Execution)
echo  [2] Synthesize Forensic Visualizations Only
echo  [3] Assemble Comprehensive PDF Audit Dossier
echo  [4] Launch Interactive Web Telemetry (Streamlit)
echo  [5] Launch Native Desktop Intelligence (Tkinter)
echo  [6] Execute Full Executive Briefing (Viz + Dossier + Web)
echo  [7] Query Environmental ^& Data Statistics
echo  [8] Purge ^& Synchronize All Artifacts
echo  [9] Open Investigative Output Directory
echo  [0] System Shutdown
echo.
echo ============================================================================
echo.

set /p choice="Command Input (0-9) > "

if "%choice%"=="1" goto RUN_PIPELINE
if "%choice%"=="2" goto RUN_VIZ
if "%choice%"=="3" goto RUN_REPORT
if "%choice%"=="4" goto RUN_STREAMLIT
if "%choice%"=="5" goto RUN_TKINTER
if "%choice%"=="6" goto RUN_DEMO
if "%choice%"=="7" goto VIEW_STATS
if "%choice%"=="8" goto CLEAN_REGEN
if "%choice%"=="9" goto OPEN_FILES
if "%choice%"=="0" goto EXIT

echo [ERROR] Invalid Command Sequence.
timeout /t 2 >nul
goto MENU

REM ============================================================================
REM Option 1: Run Holistic Pipeline
REM ============================================================================
:RUN_PIPELINE
cls
echo.
echo ============================================================================
echo                    INITIALIZING HOLISTIC PIPELINE
echo ============================================================================
echo.
echo This orchestrates the full analytical trajectory:
echo  - Phase 1: High-Volume Data Ingestion
echo  - Phase 2: Schema Fleet Unification
echo  - Phase 3: Ghost Trip Cryptanalysis
echo  - Phase 4: Probabilistic Data Imputation
echo  - Phase 5: CBD Spatial Filtration
echo  - Phase 6-7: Analytics Synthesis
echo  - Phase 8: Visualization Rendering
echo  - Phase 9: Climate Correlation
echo.
echo WARNING: High computational load and 50GB+ network ingress required.
echo.
set /p confirm="Authorize Execution? (Y/N): "
if /i not "%confirm%"=="Y" goto MENU

echo.
echo [STATUS] Launching Pipeline Orchestrator...
venv\Scripts\python.exe audit_pipeline.py

echo.
echo ============================================================================
echo [COMPLETE] Holistic Audit Sequence Finalized.
echo ============================================================================
pause
goto MENU

REM ============================================================================
REM Option 2: Synthesize Visualizations
REM ============================================================================
:RUN_VIZ
cls
echo.
echo ============================================================================
echo                    SYNTHESIZING FORENSIC VISUALS
echo ============================================================================
echo.
echo Rendering high-fidelity analytical charts:
echo  - Longitudinal oscillations
echo  - Fiscal flux dynamics
echo  - Spatial cluster distribution
echo  - Policy leakage forensics
echo.

venv\Scripts\python.exe -m src.chart_generator

echo.
echo ============================================================================
echo [COMPLETE] Visual artifacts synchronized in outputs\figures\
echo ============================================================================
pause
goto MENU

REM ============================================================================
REM Option 3: Assemble PDF Dossier
REM ============================================================================
:RUN_REPORT
cls
echo.
echo ============================================================================
echo                    ASSEMBLING AUDIT DOSSIER
echo ============================================================================
echo.
echo Orchestrating the structural assembly of the 12-page PDF report.
echo.

venv\Scripts\python.exe -m src.document_builder

echo.
echo ============================================================================
echo [COMPLETE] Audit Dossier generated: outputs\TLC_Forensic_Audit_2025.pdf
echo ============================================================================
echo Opening File...
start outputs\TLC_Forensic_Audit_2025.pdf
pause
goto MENU

REM ============================================================================
REM Option 4: Web Telemetry
REM ============================================================================
:RUN_STREAMLIT
cls
echo.
echo ============================================================================
echo                    LAUNCHING WEB TELEMETRY
echo ============================================================================
echo.
echo Initializing Streamlit Intelligence Server...
echo Address: http://localhost:8501
echo.
echo [COMMAND] Use Ctrl+C to terminate server session.
echo.

venv\Scripts\streamlit run dashboard\web_dashboard.py

pause
goto MENU

REM ============================================================================
REM Option 5: Desktop Intelligence
REM ============================================================================
:RUN_TKINTER
cls
echo.
echo ============================================================================
echo                    LAUNCHING DESKTOP INTELLIGENCE
echo ============================================================================
echo.
echo Initializing Native Forensic Interface...
echo.

venv\Scripts\python.exe dashboard\gui_dashboard.py

echo.
echo [STATUS] Session Closed.
pause
goto MENU

REM ============================================================================
REM Option 6: Executive Briefing
REM ============================================================================
:RUN_DEMO
cls
echo.
echo ============================================================================
echo                    EXECUTING BRIEFING SEQUENCE
echo ============================================================================
echo.
echo 1. Synchronizing Visual Artifacts
echo 2. Finalizing Structural PDF Dossier
echo 3. Booting Web Intelligence Interface
echo.

echo [1/3] Rendering Visuals...
venv\Scripts\python.exe -m src.chart_generator

echo.
echo [2/3] Assembling Dossier...
venv\Scripts\python.exe -m src.document_builder

echo.
echo [3/3] Deploying Web Server...
venv\Scripts\streamlit run dashboard\web_dashboard.py

pause
goto MENU

REM ============================================================================
REM Option 7: Query Statistics
REM ============================================================================
:VIEW_STATS
cls
echo.
echo ============================================================================
echo                    SYSTEM STATUS & STATISTICS
echo ============================================================================
echo.

echo [FILES] Investigating Project Volumes...
echo.

set /a src_count=0
for %%f in (src\*.py) do set /a src_count+=1
echo Core Analytical Modules: %src_count%
echo.

if exist data\raw\yellow (
    echo [DATA] Raw Ingress Volumes:
    dir /b data\raw\yellow\*.parquet 2>nul | find /c ".parquet" > temp_count.txt
    set /p yellow_count=<temp_count.txt
    echo   Yellow Fleet: !yellow_count! segments
    del temp_count.txt
)

if exist outputs\figures (
    echo.
    echo [ARTIFACTS] Forensic Visuals:
    dir /b outputs\figures\*.png 2>nul | find /c ".png" > temp_count.txt
    set /p viz_count=<temp_count.txt
    echo   High-Res Graphics: !viz_count! files
    del temp_count.txt
)

if exist outputs\TLC_Forensic_Audit_2025.pdf (
    echo.
    echo [DOSSIER] PDF Report: Synchronized
    for %%f in (outputs\TLC_Forensic_Audit_2025.pdf) do echo   Artifact Size: %%~zf bytes
)

echo.
echo [INTERFACE] Dashboard Integration:
if exist dashboard\web_dashboard.py echo   Streamlit: Operational
if exist dashboard\gui_dashboard.py echo   Tkinter: Operational

echo.
echo ============================================================================
pause
goto MENU

REM ============================================================================
REM Option 8: Purge & Synchronize
REM ============================================================================
:CLEAN_REGEN
cls
echo.
echo ============================================================================
echo                    PURGE & SYNCHRONIZATION
echo ============================================================================
echo.
echo This will delete all temporary artifacts and rebuild the forensic layers.
echo.
set /p confirm="Authorize Purge? (Y/N): "
if /i not "%confirm%"=="Y" goto MENU

echo.
echo [STATUS] Purging legacy artifacts...
if exist outputs\figures\*.png del /q outputs\figures\*.png
if exist outputs\TLC_Forensic_Audit_2025.pdf del /q outputs\TLC_Forensic_Audit_2025.pdf

echo.
echo [STATUS] Rebuilding Visualization Layer...
venv\Scripts\python.exe -m src.chart_generator

echo.
echo [STATUS] Re-assembling Structural Dossier...
venv\Scripts\python.exe -m src.document_builder

echo.
echo ============================================================================
echo [COMPLETE] System Synchronization Finalized.
echo ============================================================================
pause
goto MENU

REM ============================================================================
REM Option 9: Open Outputs
REM ============================================================================
:OPEN_FILES
cls
echo.
echo ============================================================================
echo                    INVESTIGATIVE OUTPUT ACCESS
echo ============================================================================
echo.

echo [STATUS] Opening Forensic Visuals...
if exist outputs\figures\temporal_volume_dynamics.png start outputs\figures\temporal_volume_dynamics.png
if exist outputs\figures\fiscal_trajectory_mapping.png start outputs\figures\fiscal_trajectory_mapping.png
if exist outputs\figures\spatial_load_distribution.png start outputs\figures\spatial_load_distribution.png
if exist outputs\figures\compliance_leakage_forensics.png start outputs\figures\compliance_leakage_forensics.png

timeout /t 2 >nul

echo.
echo [STATUS] Opening Structural PDF Dossier...
if exist outputs\TLC_Forensic_Audit_2025.pdf start outputs\TLC_Forensic_Audit_2025.pdf

echo.
echo Access Granted.
timeout /t 2 >nul
goto MENU

REM ============================================================================
REM System Shutdown
REM ============================================================================
:EXIT
cls
echo.
echo ============================================================================
echo           NYC FORENSIC TRANSIT AUDIT SYSTEM SHUTTING DOWN...
echo ============================================================================
echo.
echo Principal Investigator: Muhammad Umar Afzaal
echo Year: 2026
echo.
timeout /t 2 >nul
exit /b 0
