#!/usr/bin/env bash
export SPACE_PARTITIONS={{topology.partitions}}
export SPACE_HA={{topology.ha}}
if [ ${SPACE_HA} = true ]; then
    export SPACE_BACKUPS=1
    export SPACE_INSTANCES=$((${SPACE_PARTITIONS}*2))
else
    export SPACE_BACKUPS=0
    export SPACE_INSTANCES=${SPACE_PARTITIONS}
fi
