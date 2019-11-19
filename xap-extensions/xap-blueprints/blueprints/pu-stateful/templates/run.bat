@echo off
call %~dp0..\gs.bat pu run %~dp0\target\{{maven.artifactId}}-{{maven.version}}.jar %*