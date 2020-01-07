@echo off
call {{maven.artifactId}}-settings.bat
call ..\bin\gs pu undeploy {{maven.artifactId}}-space
call ..\bin\gs pu undeploy {{maven.artifactId}}-mirror
pause