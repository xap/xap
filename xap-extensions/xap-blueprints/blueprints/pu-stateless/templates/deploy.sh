#!/usr/bin/env bash
GS_HOME=${GS_HOME=`(cd ../../; pwd )`}
$GS_HOME/bin/gs.sh pu deploy {{project.artifactId}} target/{{project.artifactId}}-{{project.version}}.jar $*