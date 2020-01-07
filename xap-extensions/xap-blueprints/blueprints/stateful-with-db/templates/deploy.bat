@echo off
call {{project.artifactId}}-settings.bat
call ..\gs --cli-version=1 deploy -override-name {{project.artifactId}}-mirror -properties {{project.artifactId}}.yaml -properties embed://partitions=%{{project.artifactId}}_PARTITIONS%;backups=%{{project.artifactId}}_BACKUPS% {{project.artifactId}}-mirror-{{project.version}}.jar
call ..\gs --cli-version=1 deploy -override-name {{project.artifactId}}-space -cluster schema=partitioned total_members=%{{project.artifactId}}_PARTITIONS%,%{{project.artifactId}}_BACKUPS% -properties {{project.artifactId}}.yaml {{project.artifactId}}-space-{{project.version}}.jar
pause