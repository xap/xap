@echo off
call ..\gs pu undeploy {{project.artifactId}}-space
call ..\gs pu undeploy {{project.artifactId}}-mirror
pause