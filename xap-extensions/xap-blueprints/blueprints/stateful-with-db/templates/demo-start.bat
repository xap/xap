@echo off
call {{project.artifactId}}-env.bat

echo Building servicess (processing units)...
call build.bat

{{#db.demo.enabled}}
echo Starting HSQL DB...
start "HSQL DB (demo)" demo-db\run.bat
{{/db.demo.enabled}}

echo Creating container for mirror service (processing unit)...
call ..\gs container create --zone={{project.artifactId}}-mirror --memory={{resources.mirror.memory}} localhost
echo Creating %SPACE_INSTANCES% containers for space service (processing unit)...
call ..\gs container create --count=%SPACE_INSTANCES% --zone={{project.artifactId}}-space --memory={{resources.space.memory}} localhost

echo Deploying services (processing units)...
call deploy.bat

echo Demo start completed
pause