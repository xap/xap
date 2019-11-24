@echo off
SETLOCAL
if not defined GS_HOME set GS_HOME=%~dp0..\..
call %GS_HOME%\bin\gs.bat pu deploy {{maven.artifactId}} %~dp0\target\{{maven.artifactId}}-{{maven.version}}.jar %*