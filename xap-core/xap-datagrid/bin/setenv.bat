@echo off
rem ***********************************************************************************************************
rem * This script is used to initialize common environment to GigaSpaces XAP Server.                          *
rem * It is highly recommended NOT TO MODIFY THIS SCRIPT, to simplify future upgrades.                        *
rem * If you need to override the defaults, please modify setenv-overrides.bat or set                         *
rem * the GS_SETTINGS_FILE environment variable to your custom script.                                       *
rem * For more information see https://docs.gigaspaces.com/15.0/started/common-environment-variables.html     *
rem ***********************************************************************************************************
if not defined GS_SETTINGS_FILE set GS_SETTINGS_FILE=%~dp0\setenv-overrides.bat
if exist "%GS_SETTINGS_FILE%" call "%GS_SETTINGS_FILE%"

if defined JAVA_HOME (
	set JAVACMD="%JAVA_HOME%\bin\java"
) else (
	echo The JAVA_HOME environment variable is not set - using the java that is set in system path...
	set JAVACMD=java
)

if not defined XAP_HOME for %%d in ("%~dp0..") do set XAP_HOME=%%~fd
if not defined GS_HOME set GS_HOME=%XAP_HOME%

if not defined XAP_NIC_ADDRESS set XAP_NIC_ADDRESS=%COMPUTERNAME%
if not defined GS_NIC_ADDRESS set GS_NIC_ADDRESS=%XAP_NIC_ADDRESS%
