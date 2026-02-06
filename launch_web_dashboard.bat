@echo off
REM ============================================================================
REM Streamlit Dashboard Launcher with Clean Restart
REM ============================================================================

echo [STATUS] Terminating any existing Streamlit processes...
taskkill /F /IM streamlit.exe 2>nul
timeout /t 1 >nul

echo [STATUS] Clearing browser cache recommendation...
echo Please clear your browser cache (Ctrl+Shift+Delete) or open in incognito mode
echo.
timeout /t 2 >nul

echo [STATUS] Launching Web Telemetry Dashboard...
echo.
venv\Scripts\streamlit run dashboard\web_dashboard.py

pause
