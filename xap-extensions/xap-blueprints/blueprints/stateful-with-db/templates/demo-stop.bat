@echo off
echo Undeploying processing units...
call undeploy.bat

echo TODO: Stop kill GSCs with zones {{project.artifactId}}-space, {{project.artifactId}}-mirror

{{#db.demo.enabled}}
echo Stopping HSQL DB...
call demo-db\shutdown.bat
{{/db.demo.enabled}}

echo Demo stop completed
pause