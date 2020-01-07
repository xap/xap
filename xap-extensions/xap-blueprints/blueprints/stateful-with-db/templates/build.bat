@echo off
call mvn package
move {{maven.artifactId}}-space\target\{{maven.artifactId}}-space-{{maven.version}}.jar .
move {{maven.artifactId}}-mirror\target\{{maven.artifactId}}-mirror-{{maven.version}}.jar .