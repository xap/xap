@echo off
call {{maven.artifactId}}-settings.bat
call ..\bin\gs --cli-version=1 deploy -override-name {{maven.artifactId}}-mirror -properties {{maven.artifactId}}.yaml -properties embed://partitions=%{{maven.artifactId}}_PARTITIONS%;backups=%{{maven.artifactId}}_BACKUPS% {{maven.artifactId}}-mirror-{{maven.version}}.jar
call ..\bin\gs --cli-version=1 deploy -override-name {{maven.artifactId}}-space -cluster schema=partitioned total_members=%{{maven.artifactId}}_PARTITIONS%,%{{maven.artifactId}}_BACKUPS% -properties {{maven.artifactId}}.yaml {{maven.artifactId}}-space-{{maven.version}}.jar
pause