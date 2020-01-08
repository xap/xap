@echo off
if exist target (
    echo Purging existing files from target...	
	rd /s /q target
)
call mvn package
md target
move {{project.artifactId}}-space\target\{{project.artifactId}}-space-{{project.version}}.jar target\
move {{project.artifactId}}-mirror\target\{{project.artifactId}}-mirror-{{project.version}}.jar target\