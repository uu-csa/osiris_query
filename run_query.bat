@echo off

set CONDA_FORCE_32BIT = 1
call activate py32

@echo ======================================================
@echo               O S I R I S   Q U E R Y
@echo ------------------------------------------------------
@echo  Selecteer de query set die je wilt uitvoeren:
@echo  [1] q_monitor            var: collegejaar
@echo  [2] q_vooropleiding      var: collegejaar
@echo  [3] q_betaalmail         var: collegejaar
@echo  [4] q_ooa_dossier        var: proces, status_besluit
@echo  [5] q_ad_hoc             var: query naam
@echo  [6] q_referentie         var: -
@echo  [7] q_referentie_ooa     var: -
@echo ======================================================

set /p selection=Query set: 
set /p vars=Geef variabelen op: 

if %selection% == 1 set query=q_monitor
if %selection% == 2 set query=q_vooropleiding
if %selection% == 3 set query=q_betaalmail
if %selection% == 4 set query=q_ooa_dossier
if %selection% == 5 set query=q_ad_hoc
if %selection% == 6 set query=q_referentie
if %selection% == 7 set query=q_referentie_ooa

@echo.
@echo Running: python.exe %query%.py %vars%
@echo.

call python.exe %query%.py %vars%

@echo.
pause
