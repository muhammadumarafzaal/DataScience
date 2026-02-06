@echo off
echo ============================================================
echo   NYC FORENSIC TRANSIT AUDIT 2025 - SYSTEM STAGING
echo   Lead Architect: Muhammad Umar Afzaal (23F-3106)
echo ============================================================
echo.

if not exist venv (
    echo [STATUS] Initializing Virtual Environment Cluster...
    python -m venv venv
)

echo [STATUS] Synchronizing Dependency Matrix...
venv\Scripts\python -m pip install --upgrade pip
venv\Scripts\pip install -r requirements.txt

echo.
echo [COMPLETE] System Staging Finalized.
echo Execute audit_manager.bat to initialize the forensic pipeline.
pause
