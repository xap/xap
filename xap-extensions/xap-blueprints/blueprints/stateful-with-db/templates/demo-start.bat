@echo off
call {{project.artifactId}}-env.bat
if not exist target (
	echo Building processing units...
	call build.bat
)

echo Starting HSQL DB...
start "HSQL DB (demo)" demo-db\run.bat

echo Creating container for mirror processing unit...
call ..\gs container create --zone={{project.artifactId}}-mirror --memory={{resources.mirror.memory}} localhost
echo Creating %SPACE_INSTANCES% containers for space processing unit...
FOR /L %%G IN (1,1,%SPACE_INSTANCES%) DO call ..\gs container create --zone={{project.artifactId}}-space --memory={{resources.space.memory}} localhost

echo Deploying processing units...
call deploy.bat

echo Demo start completed
pause