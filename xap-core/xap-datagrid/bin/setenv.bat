@echo off
rem ***********************************************************************************************************
rem * This script is used to initialize common environment to GigaSpaces XAP Server.                          *
rem * It is highly recommended NOT TO MODIFY THIS SCRIPT, to simplify future upgrades.                        *
rem * If you need to override the defaults, please modify setenv-overrides.bat or set                         *
rem * the XAP_SETTINGS_FILE environment variable to your custom script.                                       *
rem * For more information see http://docs.gigaspaces.com/xap/12.2/started/common-environment-variables.html *
rem ***********************************************************************************************************
if not defined XAP_SETTINGS_FILE set XAP_SETTINGS_FILE=%~dp0\setenv-overrides.bat
if exist "%XAP_SETTINGS_FILE%" call "%XAP_SETTINGS_FILE%"

if defined JAVA_HOME (
	set JAVACMD="%JAVA_HOME%\bin\java"
	set JAVACCMD="%JAVA_HOME%\bin\javac"
	set JAVAWCMD="%JAVA_HOME%\bin\javaw"
) else (
	echo The JAVA_HOME environment variable is not set - using the java that is set in system path...
	set JAVACMD=java
	set JAVACCMD=javac
	set JAVAWCMD=javaw
)

if defined XAP_HOME goto XAP_HOME_DEFINED
pushd %~dp0..
set XAP_HOME=%CD%
popd
:XAP_HOME_DEFINED

if not defined XAP_NIC_ADDRESS set XAP_NIC_ADDRESS=%COMPUTERNAME%
if not defined XAP_SECURITY_POLICY set XAP_SECURITY_POLICY=%XAP_HOME%\policy\policy.all
if not defined XAP_LOGS_CONFIG_FILE set XAP_LOGS_CONFIG_FILE=%XAP_HOME%\config\log\xap_logging.properties

set XAP_OPTIONS=-Djava.util.logging.config.file="%XAP_LOGS_CONFIG_FILE%" -Djava.security.policy="%XAP_SECURITY_POLICY%" -Djava.rmi.server.hostname="%XAP_NIC_ADDRESS%" -Dcom.gs.home="%XAP_HOME%"

if not defined JAVA_OPTIONS (
	pushd "%XAP_HOME%\lib\required"
	FOR /F "tokens=*" %%i IN ('%JAVACMD% -cp xap-datagrid.jar com.gigaspaces.internal.utils.OutputJVMOptions') DO set JAVA_OPTIONS=%%i %EXT_JAVA_OPTIONS%
	popd
)

set GS_JARS="%XAP_HOME%\lib\platform\ext\*";"%XAP_HOME%";"%XAP_HOME%\lib\required\*";"%XAP_HOME%\lib\optional\pu-common\*";"%XAP_CLASSPATH_EXT%"
set COMMONS_JARS="%XAP_HOME%\lib\platform\commons\*;"
set JDBC_JARS="%XAP_HOME%\lib\optional\jdbc\*;"
set SIGAR_JARS="%XAP_HOME%\lib\optional\sigar\*;"
set SPRING_JARS="%XAP_HOME%\lib\optional\spring\*;%XAP_HOME%\lib\optional\security\*;"

if "%VERBOSE%"=="true" (
	echo ===============================================================================
	echo GigaSpaces XAP environment verbose information
	echo XAP_HOME: %XAP_HOME%
	echo XAP_NIC_ADDRESS: %XAP_NIC_ADDRESS%
	echo XAP_LOOKUP_GROUPS: %XAP_LOOKUP_GROUPS%
	echo XAP_LOOKUP_LOCATORS: %XAP_LOOKUP_LOCATORS%
	echo GS_JARS: %GS_JARS%
	echo.
	echo JAVA_HOME: %JAVA_HOME%
	echo EXT_JAVA_OPTIONS: %EXT_JAVA_OPTIONS%
	echo JAVA_OPTIONS: %JAVA_OPTIONS%
	echo ===============================================================================
)
