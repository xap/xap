@echo off
call {{project.artifactId}}-env.bat
call ..\gs pu deploy --properties={{project.artifactId}}-values.yaml --zones={{project.artifactId}}-mirror -p --ha=%SPACE_HA% {{project.artifactId}}-mirror target\{{project.artifactId}}-mirror-{{project.version}}.jar
call ..\gs pu deploy --properties={{project.artifactId}}-values.yaml --zones={{project.artifactId}}-space --partitions=%SPACE_PARTITIONS% --ha=%SPACE_HA% -p pu.dynamic-partitioning=%SPACE_HA% {{project.artifactId}}-space target\{{project.artifactId}}-space-{{project.version}}.jar