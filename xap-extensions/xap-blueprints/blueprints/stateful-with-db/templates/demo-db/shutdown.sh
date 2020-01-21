#!/usr/bin/env bash
. `dirname $0`/env.sh
${GS_DB_CLI} shutdown --user sa jdbc:hsqldb:hsql://localhost/${DB_NAME}