@echo off
set today=%date:~0,4%-%date:~5,2%-%date:~8,2%
for /f %%x in ('wmic path win32_utctime get /format:list ^| findstr "="') do set %%x
@call :output>install%today%_%Hour%%Minute%%Second%.log
exit
:output

for /f %%i in ('git tag') do set tag1=%%i
for /f %%j in ('git tag') do set tag2=%%j

IF %tag1%==%tag2% (
    call install_run.bat
) ELSE (
    git reset --hard %tag1%
    call uninstall.bat
    git pull origin master
    call install_run.bat
)