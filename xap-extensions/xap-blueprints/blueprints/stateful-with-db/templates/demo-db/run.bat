@echo off
call %~dp0\env.bat
%GS_DB_CLI% run-hsqldb %DB_NAME% --path %~dp0\db --init-script %~dp0\init-demo-db.sql