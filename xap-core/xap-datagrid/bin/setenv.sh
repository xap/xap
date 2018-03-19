#!/bin/bash
# ***********************************************************************************************************
# * This script is used to initialize common environment to GigaSpaces XAP Server.                          *
# * It is highly recommended NOT TO MODIFY THIS SCRIPT, to simplify future upgrades.                        *
# * If you need to override the defaults, please modify setenv-overrides.sh or set                          *
# * the XAP_SETTINGS_FILE environment variable to your custom script.                                       *
# * For more information see http://docs.gigaspaces.com/xap/12.3/started/common-environment-variables.html *
# ***********************************************************************************************************
#Load overrides settings.
DIRNAME=$(dirname ${BASH_SOURCE[0]})

if [ -z ${XAP_SETTINGS_FILE} ]; then
    export XAP_SETTINGS_FILE=${DIRNAME}/setenv-overrides.sh
fi

if [ -f ${XAP_SETTINGS_FILE} ]; then
    source ${XAP_SETTINGS_FILE}
fi

if [ -z "${JAVA_HOME}" ]; then
  	echo "The JAVA_HOME environment variable is not set. Using the java that is set in system path."
	export JAVACMD=java
	export JAVACCMD=javac
	export JAVAWCMD=javaw
else
	export JAVACMD="${JAVA_HOME}/bin/java"
	export JAVACCMD="${JAVA_HOME}/bin/javac"
	export JAVAWCMD="${JAVA_HOME}/bin/javaw"
fi

export XAP_HOME=${XAP_HOME=`(cd $DIRNAME/..; pwd )`}
export XAP_NIC_ADDRESS=${XAP_NIC_ADDRESS="`hostname`"}
export XAP_SECURITY_POLICY=${XAP_SECURITY_POLICY=${XAP_HOME}/policy/policy.all}
export XAP_LOGS_CONFIG_FILE=${XAP_LOGS_CONFIG_FILE=${XAP_HOME}/config/log/xap_logging.properties}
export XAP_OPTIONS="-Djava.util.logging.config.file=${XAP_LOGS_CONFIG_FILE} -Djava.security.policy=${XAP_SECURITY_POLICY} -Djava.rmi.server.hostname=${XAP_NIC_ADDRESS} -Dcom.gs.home=${XAP_HOME}"
export EXT_LD_LIBRARY_PATH=${EXT_LD_LIBRARY_PATH=}

if [ -z "${JAVA_OPTIONS}" ]; then
   JAVA_OPTIONS=`${JAVACMD} -cp "${XAP_HOME}/lib/required/xap-datagrid.jar" com.gigaspaces.internal.utils.OutputJVMOptions`
   JAVA_OPTIONS="${JAVA_OPTIONS} ${EXT_JAVA_OPTIONS}"
fi
export JAVA_OPTIONS

export GS_JARS="${XAP_HOME}"/lib/platform/ext/*:${XAP_HOME}:"${XAP_HOME}"/lib/required/*:"${XAP_HOME}"/lib/optional/pu-common/*:"${XAP_CLASSPATH_EXT}"
export COMMONS_JARS="${XAP_HOME}"/lib/platform/commons/*
export JDBC_JARS="${XAP_HOME}"/lib/optional/jdbc/*
export SIGAR_JARS="${XAP_HOME}"/lib/optional/sigar/*
export SPRING_JARS="${XAP_HOME}"/lib/optional/spring/*:"${XAP_HOME}"/lib/optional/security/*

if [ "${VERBOSE}" = "true" ] ; then
	echo ===============================================================================
	echo GigaSpaces XAP environment verbose information
	echo XAP_HOME: $XAP_HOME
	echo XAP_NIC_ADDRESS: $XAP_NIC_ADDRESS
	echo XAP_LOOKUP_GROUPS: $XAP_LOOKUP_GROUPS
	echo XAP_LOOKUP_LOCATORS: $XAP_LOOKUP_LOCATORS
	echo GS_JARS: $GS_JARS
	echo
	echo JAVA_HOME: $JAVA_HOME
	echo JAVA_VM_NAME: $JAVA_VM_NAME
	echo JAVA_VERSION: $JAVA_VERSION
	echo EXT_JAVA_OPTIONS: $EXT_JAVA_OPTIONS
	echo JAVA_OPTIONS: $JAVA_OPTIONS
	echo ===============================================================================
fi
