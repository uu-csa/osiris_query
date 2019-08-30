@echo off

set CONDA_FORCE_32BIT = 1
call activate py32
call python.exe run_query.py

@echo.
