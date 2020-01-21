#!/usr/bin/env bash
if [ -e target ]; then
    echo "Purging existing files from target..."
    rm -r target
fi
mvn package
mkdir target
mv {{project.artifactId}}-space/target/{{project.artifactId}}-space-{{project.version}}.jar target/
mv {{project.artifactId}}-mirror/target/{{project.artifactId}}-mirror-{{project.version}}.jar target/