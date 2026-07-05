@echo off
REM Emerging Edge — Windows auto-start script (equivalent of start-server.sh)
REM Double-click this file, or run it from a terminal.

REM %~dp0 is the directory this .bat file lives in (with trailing
REM backslash) — makes the script portable across machines/paths,
REM unlike the Mac script's hardcoded /Users/... path.
cd /d "%~dp0"

REM Load SERPER_API_KEY (and anything else) from .env if present.
REM Expects simple KEY=VALUE lines, one per line, no quotes.
if exist ".env" (
    for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
        set "%%A=%%B"
    )
)

python monitor.py serve
