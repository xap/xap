@echo off
call %~dp0\env.bat
call %GS_DB_CLI% shutdown --user sa jdbc:hsqldb:hsql://localhost/%DB_NAME%