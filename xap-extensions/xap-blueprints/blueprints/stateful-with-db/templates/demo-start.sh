#!/usr/bin/env bash
. ./{{project.artifactId}}-env.sh
if [ ! -e target ]; then
    echo "Building processing units..."
    ./build.sh
fi

{{#db.demo.enabled}}
echo "Starting HSQL DB..."
xterm -e ./demo-db/run.sh &
{{/db.demo.enabled}}

echo "Creating container for mirror processing unit..."
../gs.sh container create --zone={{project.artifactId}}-mirror --memory={{resources.mirror.memory}} localhost
echo "Creating $SPACE_INSTANCES containers for space processing unit..."
for (( c=1; c<=$SPACE_INSTANCES; c++ ))
do
    ../gs.sh container create --zone={{project.artifactId}}-space --memory={{resources.space.memory}} localhost
done

echo "Deploying processing units..."
./deploy.sh

echo "Demo start completed"
