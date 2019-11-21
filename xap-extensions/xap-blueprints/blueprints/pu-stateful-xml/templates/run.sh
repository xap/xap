#!/usr/bin/env bash
if [ -z "$GS_HOME" ]; then
    export GS_HOME=$(cd ../../ ; pwd)
fi

$GS_HOME/bin/gs.sh pu run target/{{maven.artifactId}}-{{maven.version}}.jar $*