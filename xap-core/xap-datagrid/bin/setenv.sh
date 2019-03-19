#!/usr/bin/env bash
# ***********************************************************************************************************
# * This script is used to initialize common environment to GigaSpaces XAP Server.                          *
# * It is highly recommended NOT TO MODIFY THIS SCRIPT, to simplify future upgrades.                        *
# * If you need to override the defaults, please modify setenv-overrides.sh or set                          *
# * the XAP_SETTINGS_FILE environment variable to your custom script.                                       *
# * For more information see https://docs.gigaspaces.com/14.2/started/common-environment-variables.html *
# ***********************************************************************************************************
#Load overrides settings.
DIRNAME=$(dirname ${BASH_SOURCE[0]})

export XAP_SETTINGS_FILE=${XAP_SETTINGS_FILE=${DIRNAME}/setenv-overrides.sh}
if [ -f ${XAP_SETTINGS_FILE} ]; then
    source ${XAP_SETTINGS_FILE}
fi

if [ -z "${JAVA_HOME}" ]; then
  	echo "The JAVA_HOME environment variable is not set. Using the java that is set in system path."
	export JAVACMD=java
else
	export JAVACMD="${JAVA_HOME}/bin/java"
fi

export XAP_HOME=${XAP_HOME=`(cd $DIRNAME/..; pwd )`}
export XAP_NIC_ADDRESS=${XAP_NIC_ADDRESS="`hostname`"}

if [ "${VERBOSE}" = "true" ] ; then
	echo ===============================================================================
	echo GigaSpaces XAP environment verbose information
	echo XAP_HOME: $XAP_HOME
	echo XAP_NIC_ADDRESS: $XAP_NIC_ADDRESS
	echo XAP_LOOKUP_GROUPS: $XAP_LOOKUP_GROUPS
	echo XAP_LOOKUP_LOCATORS: $XAP_LOOKUP_LOCATORS
	echo
	echo JAVA_HOME: $JAVA_HOME
	echo EXT_JAVA_OPTIONS: $EXT_JAVA_OPTIONS
	echo ===============================================================================
fi
