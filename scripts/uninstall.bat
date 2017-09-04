@echo off
for /f %%x in ('wmic path win32_utctime get /format:list ^| findstr "="') do set %%x

for /f "delims=" %%i in ('call uninstall_run.bat') do (
echo [%Year%-%Month%-%Day% %Hour%:%Minute%:%Second%] %%i >> uninstall%Year%-%Month%-%Day%_%Hour%%Minute%%Second%.log
)
