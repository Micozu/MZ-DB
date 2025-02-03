@echo off
REM Install dependencies from requirements.txt
echo Installing dependencies...
pip install -r requirements.txt

REM Run the mz-db.py application without a console window
echo Running MZ-DB Application...
pythonw mz-db.py
