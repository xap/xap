@echo off
call {{maven.artifactId}}-settings.bat
call ..\bin\gs host run-agent --auto --gsc=%{{maven.artifactId}}_GSC%
