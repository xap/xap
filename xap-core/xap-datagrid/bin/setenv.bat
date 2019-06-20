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

if defined XAP_HOME goto XAP_HOME_DEFINED
pushd %~dp0..
set XAP_HOME=%CD%
popd
:XAP_HOME_DEFINED

if not defined XAP_NIC_ADDRESS set XAP_NIC_ADDRESS=%COMPUTERNAME%

if "%VERBOSE%"=="true" (
	echo ===============================================================================
	echo GigaSpaces XAP environment verbose information
	echo XAP_HOME: %XAP_HOME%
	echo XAP_NIC_ADDRESS: %XAP_NIC_ADDRESS%
	echo XAP_LOOKUP_GROUPS: %XAP_LOOKUP_GROUPS%
	echo XAP_LOOKUP_LOCATORS: %XAP_LOOKUP_LOCATORS%
	echo.
	echo JAVA_HOME: %JAVA_HOME%
	echo EXT_JAVA_OPTIONS: %EXT_JAVA_OPTIONS%
	echo ===============================================================================
)
