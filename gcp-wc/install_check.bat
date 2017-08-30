for /f %%i in ('git tag') do set tag1=%%i
echo %tag1%
git pull origin master
for /f %%j in ('git tag') do set tag2=%%j
echo %tag2%

)