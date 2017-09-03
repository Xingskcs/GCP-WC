for /f %%i in ('git tag') do set tag1=%%i
git pull origin master
for /f %%j in ('git tag') do set tag2=%%j

echo %tag1%
echo %tag2%