@echo off
call "%~dp0\setenv.bat"
set XAP_CLI_CP="%XAP_HOME%\tools\cli\*";%GS_JARS%
java %XAP_OPTIONS% -cp %XAP_CLI_CP% org.gigaspaces.cli.commands.XapMainCommand %*