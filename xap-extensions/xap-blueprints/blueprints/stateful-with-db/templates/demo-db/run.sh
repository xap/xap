#!/usr/bin/env bash
. `dirname $0`/env.sh
${GS_DB_CLI} run-hsqldb ${DB_NAME} --path `dirname $0`/db --init-script `dirname $0`/init-demo-db.sql