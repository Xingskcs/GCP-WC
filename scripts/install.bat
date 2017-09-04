@echo off
for /f %%x in ('wmic path win32_utctime get /format:list ^| findstr "="') do set %%x

for /f "delims=" %%i in ('call install_run.bat') do (
echo [%Year%-%Month%-%Day% %Hour%:%Minute%:%Second%] %%i >> install%Year%-%Month%-%Day%_%Hour%%Minute%%Second%.log
)

