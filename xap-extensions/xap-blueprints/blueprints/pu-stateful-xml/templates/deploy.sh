#!/usr/bin/env bash
if [ -z "$GS_HOME" ]; then
    export GS_HOME=$(cd ../../ ; pwd)
fi

$GS_HOME/bin/gs.sh pu deploy {{maven.artifactId}} target/{{maven.artifactId}}-{{maven.version}}.jar $*