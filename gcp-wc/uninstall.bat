@echo off
set today=%date:~0,4%-%date:~5,2%-%date:~8,2%
for /f %%x in ('wmic path win32_utctime get /format:list ^| findstr "="') do set %%x
@call :output>uninstall%today%_%Hour%%Minute%%Second%.log
exit
:output

call uninstall_run.bat
