@echo off
echo Undeploying processing units...
call undeploy.bat

echo TODO: Stop kill GSCs with zones {{project.artifactId}}-space, {{project.artifactId}}-mirror

echo Stopping HSQL DB...
call demo-db\shutdown.bat

echo Demo stop completed
pause