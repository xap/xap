#!/usr/bin/env bash
# ***********************************************************************************************************
# * This script is used to initialize common environment to GigaSpaces XAP Server.                          *
# * It is highly recommended NOT TO MODIFY THIS SCRIPT, to simplify future upgrades.                        *
# * If you need to override the defaults, please modify setenv-overrides.sh or set                          *
# * the GS_SETTINGS_FILE environment variable to your custom script.                                       *
# * For more information see https://docs.gigaspaces.com/15.2/started/common-environment-variables.html     *
# ***********************************************************************************************************
#Load overrides settings.
DIRNAME=$(dirname ${BASH_SOURCE[0]})

export GS_SETTINGS_FILE=${GS_SETTINGS_FILE=${DIRNAME}/setenv-overrides.sh}
if [ -f ${GS_SETTINGS_FILE} ]; then
    source ${GS_SETTINGS_FILE}
fi

if [ -z "${JAVA_HOME}" ]; then
  	echo "The JAVA_HOME environment variable is not set. Using the java that is set in system path."
	export JAVACMD=java
else
	export JAVACMD="${JAVA_HOME}/bin/java"
fi

export XAP_HOME=${XAP_HOME=`(cd $DIRNAME/..; pwd )`}
export GS_HOME=${GS_HOME=${XAP_HOME}}

export XAP_NIC_ADDRESS=${XAP_NIC_ADDRESS="`hostname`"}
export GS_NIC_ADDRESS=${GS_NIC_ADDRESS=${XAP_NIC_ADDRESS}}
