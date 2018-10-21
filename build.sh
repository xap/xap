#!/usr/bin/env bash
# ************************************************************************************************
# * This script is used to build XAP.                                                            *
# * Example: ./build.sh -T 1C -Dmaven.repo.local=/home/kuku/my_repo                              *
# ************************************************************************************************
DIRNAME=$(dirname ${BASH_SOURCE[0]})
pushd ${DIRNAME}
echo "Building XAP"
mvn clean install $*
EXIT_CODE=$?
if [ ${EXIT_CODE} -ne 0 ]; then
	echo "Build failed!"
	popd
	exit 1
fi
popd
echo "Finished building XAP"