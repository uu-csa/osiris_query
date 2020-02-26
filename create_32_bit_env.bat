call conda remove --name py32 --all
set CONDA_FORCE_32BIT=1
call conda env create -f environment.yml
pause
