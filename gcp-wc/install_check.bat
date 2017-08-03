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