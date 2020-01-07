@echo off
call {{project.artifactId}}-settings.bat
call ..\gs host run-agent --auto --gsc=%{{project.artifactId}}_GSC%
