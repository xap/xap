#!/usr/bin/env bash
echo "Undeploying processing units..."
./undeploy.sh

echo "TODO: Stop kill GSCs with zones {{project.artifactId}}-space, {{project.artifactId}}-mirror"

{{#db.demo.enabled}}
echo "Stopping HSQL DB..."
./demo-db/shutdown.sh
{{/db.demo.enabled}}

echo "Demo stop completed"