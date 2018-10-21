#!/usr/bin/env bash

#########################################################################################
# This scrip greps through the currently running processes and lists the process IDs 
# which matches the selection criteria to stdout. All matched process with supplied 
# PROCESS_ID will be killed with: kill -9 
#
# NOTE: The PROCESS_ID does match with process arguments as well.
#
# @author Igor Goldenberg
# @version 1.0
#########################################################################################

PROCESS_ID=$1

if [ "${PROCESS_ID}" = "" ] ; then
  echo "$0 [Process ID/NAME]"  
  exit	
fi

#extract OS name with lowercase
OS=`uname -s | tr '[A-Z]' '[a-z]'`

# port ps command on different Unix based systems
if [ "$OS" = "sunos" ] ; then
  PSFLAG="-efo user,pid,ppid,args"
else
  PSFLAG="-eawwo user,pid,ppid,command"   
fi

#kill all parent and children processes 
echo "Kill all [${PROCESS_ID}] processes..."
ps ${PSFLAG} | grep ${PROCESS_ID} | grep -v grep | awk '{print $2}' | xargs kill -9 
