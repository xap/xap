#!/usr/bin/env bash
. `dirname $0`/setenv.sh
GS_COMMAND="`$JAVACMD -cp "${GS_HOME}/lib/required/*" com.gigaspaces.start.GsCommandFactory cli` $*"
if [[ $GS_COMMAND == Error:* ]]; then
    echo $GS_COMMAND
else
    if [[ $GS_VERBOSE == "true" ]]; then
        echo Executing GigaSpaces command:
        echo $GS_COMMAND
        echo --------------------------------------------------------------------------------
    fi
    $GS_COMMAND
fi
