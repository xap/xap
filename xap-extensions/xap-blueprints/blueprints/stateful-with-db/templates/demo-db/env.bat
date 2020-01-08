@echo off
call "%~dp0\..\..\setenv.bat"
set DB_NAME={{db.name}}
set GS_DB_CLI=%JAVACMD% -cp %GS_HOME%\tools\cli\*;%GS_HOME%\lib\required\*;%GS_HOME%\lib\optional\jdbc\* com.gigaspaces.cli.commands.db.DbCommand