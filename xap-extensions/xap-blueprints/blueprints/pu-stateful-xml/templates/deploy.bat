@echo off
call %~dp0..\gs.bat pu deploy {{maven.artifactId}} %~dp0\target\{{maven.artifactId}}-{{maven.version}}.jar %*