@echo off
echo Undeploying processing units...
call undeploy.bat

echo TODO: Stop relevant GSCs.

echo Stopping HSQL DB...
call demo-db\shutdown.bat

echo Demo stop completed
pause