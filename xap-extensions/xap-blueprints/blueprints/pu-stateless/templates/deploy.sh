#!/usr/bin/env bash
GS_HOME=${GS_HOME=`(cd ../../; pwd )`}
$GS_HOME/bin/gs.sh pu deploy {{maven.artifactId}} target/{{maven.artifactId}}-{{maven.version}}.jar $*