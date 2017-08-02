@echo off
set today=%date:~0,4%-%date:~5,2%-%date:~8,2%
@call :output>uninstall%today%_%time:~0,2%%time:~3,2%%time:~6,2%.log
exit
:output

call uninstall_run.bat
