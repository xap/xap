#!/usr/bin/env bash
. ./{{project.artifactId}}-env.sh

echo "Building services (processing units)..."
./build.sh

{{#db.demo.enabled}}
echo "Starting HSQL DB..."
xterm -e ./demo-db/run.sh &
{{/db.demo.enabled}}

echo "Creating container for mirror service (processing unit)..."
../gs.sh container create --zone={{project.artifactId}}-mirror --memory={{resources.mirror.memory}} localhost
echo "Creating $SPACE_INSTANCES containers for space service (processing unit)..."
../gs.sh container create --count=$SPACE_INSTANCES --zone={{project.artifactId}}-space --memory={{resources.space.memory}} localhost

echo "Deploying services (processing units)..."
./deploy.sh

echo "Demo start completed"
