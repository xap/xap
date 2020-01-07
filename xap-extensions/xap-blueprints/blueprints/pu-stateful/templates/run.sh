#!/usr/bin/env bash
GS_HOME=${GS_HOME=`(cd ../../; pwd )`}
$GS_HOME/bin/gs.sh pu run target/{{project.artifactId}}-{{project.version}}.jar $*