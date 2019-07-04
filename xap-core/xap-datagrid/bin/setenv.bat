@echo off
rem ***********************************************************************************************************
rem * This script is used to initialize common environment to GigaSpaces XAP Server.                          *
rem * It is highly recommended NOT TO MODIFY THIS SCRIPT, to simplify future upgrades.                        *
rem * If you need to override the defaults, please modify setenv-overrides.bat or set                         *
rem * the XAP_SETTINGS_FILE environment variable to your custom script.                                       *
rem * For more information see https://docs.gigaspaces.com/14.5/started/common-environment-variables.html     *
rem ***********************************************************************************************************
if not defined XAP_SETTINGS_FILE set XAP_SETTINGS_FILE=%~dp0\setenv-overrides.bat
if exist "%XAP_SETTINGS_FILE%" call "%XAP_SETTINGS_FILE%"

if defined JAVA_HOME (
	set JAVACMD="%JAVA_HOME%\bin\java"
) else (
	echo The JAVA_HOME environment variable is not set - using the java that is set in system path...
	set JAVACMD=java
)

if not defined XAP_HOME for %%d in ("%~dp0..") do set XAP_HOME=%%~fd
if not defined XAP_NIC_ADDRESS set XAP_NIC_ADDRESS=%COMPUTERNAME%
