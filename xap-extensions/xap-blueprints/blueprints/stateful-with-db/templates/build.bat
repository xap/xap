@echo off
call mvn package
move {{project.artifactId}}-space\target\{{project.artifactId}}-space-{{project.version}}.jar .
move {{project.artifactId}}-mirror\target\{{project.artifactId}}-mirror-{{project.version}}.jar .