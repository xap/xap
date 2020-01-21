#!/usr/bin/env bash
echo "Undeploying processing units..."
./undeploy.sh

echo "TODO: Stop kill GSCs with zones {{project.artifactId}}-space, {{project.artifactId}}-mirror"

echo "Stopping HSQL DB..."
./demo-db/shutdown.sh

echo "Demo stop completed"