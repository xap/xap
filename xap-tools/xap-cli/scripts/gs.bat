@echo off
if not defined XAP_HOME set XAP_HOME=%~dp0..\..
set XAP_CLI_CP=*;"%XAP_HOME%\lib\required\xap-common.jar"
java -cp %XAP_CLI_CP% org.gigaspaces.cli.commands.XapMainCommand %*
