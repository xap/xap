@echo off
echo Undeploying services (processing units)...
call undeploy.bat

echo Killing GSCs with zones {{project.artifactId}}-space, {{project.artifactId}}-mirror
call ..\gs container kill --zones={{project.artifactId}}-space,{{project.artifactId}}-mirror

{{#db.demo.enabled}}
echo Stopping HSQL DB...
call demo-db\shutdown.bat
{{/db.demo.enabled}}

echo Demo stop completed
pause