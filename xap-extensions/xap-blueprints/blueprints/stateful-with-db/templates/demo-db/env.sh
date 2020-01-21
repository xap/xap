#!/usr/bin/env bash
. `dirname $0`/../../setenv.sh

export DB_NAME={{db.name}}
export GS_DB_CLI="${JAVACMD} -cp ${GS_HOME}/tools/cli/*:${GS_HOME}/lib/required/*:${GS_HOME}/lib/optional/jdbc/* com.gigaspaces.cli.commands.db.DbCommand"