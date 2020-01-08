@echo off
call {{project.artifactId}}-settings.bat
if not exist target (
	echo Building processing units...
	call build.bat
)

echo Starting HSQL DB...
start "HSQL DB (demo)" demo-db\run.bat

echo Creating container for mirror processing unit...
call ..\gs container create --zone={{project.artifactId}}-mirror localhost
echo Creating %{{project.artifactId}}_INSTANCES% containers for space processing unit...
FOR /L %%G IN (1,1,%{{project.artifactId}}_INSTANCES%) DO call ..\gs container create --zone={{project.artifactId}}-space localhost

echo Deploying processing units...
call deploy.bat

echo Demo start completed
pause