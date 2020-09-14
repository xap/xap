/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gigaspaces.internal.remoting.routing.partitioned;

import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.utils.GsEnv;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class PartitionedClusterUtils {
    public static final int NO_PARTITION = -1;
    private static final boolean PRECISE_LONG_ROUTING = !Boolean.getBoolean("com.gs.disable-precise-long-routing");

    public static final String DYNAMIC_PARTITIONING_PROPERTY = "pu.dynamic-partitioning";
    private static final boolean DYNAMIC_PARTITIONING_DEFAULT = GsEnv.propertyBoolean("com.gs.pu.dynamic-partitioning").get(false);

    public static boolean isDynamicPartitioningEnabled(String propValue, boolean isPartitioned) {
        if (propValue != null)
            return Boolean.parseBoolean(propValue);
        return isPartitioned ? DYNAMIC_PARTITIONING_DEFAULT : false;
    }

    private PartitionedClusterUtils() {
    }

    public static int getPartitionId(Object routingValue, SpaceClusterInfo clusterInfo) {
        if (routingValue == null)
            return NO_PARTITION;
        if (routingValue instanceof Long && PRECISE_LONG_ROUTING) {
            return clusterInfo.isChunksRouting() ? clusterInfo.getPartitionId((safeAbs((Long) routingValue))) : (int) (safeAbs((Long) routingValue) % clusterInfo.getNumberOfPartitions());
        }
        return clusterInfo.isChunksRouting() ? clusterInfo.getPartitionId(safeAbs(routingValue.hashCode())) : safeAbs(routingValue.hashCode()) % clusterInfo.getNumberOfPartitions();
    }

    public static int getPartitionId(Object routingValue, ClusterTopology topology) {
        if (routingValue == null)
            return NO_PARTITION;
        if (routingValue instanceof Long && PRECISE_LONG_ROUTING) {
            return  topology.getPartitionId((safeAbs((Long) routingValue)));
        }
        return topology.getPartitionId(safeAbs(routingValue.hashCode()));
    }

    public static int safeAbs(int value) {
        return value == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(value);
    }

    private static long safeAbs(long value) {
        return value == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(value);
    }

    public static int extractPartitionIdFromSpaceName(String spaceName) {
        // Format: qaSpace_container2_1:qaSpace
        if (spaceName == null || spaceName.length() == 0)
            return -1;

        final String partitionPrefix = "_container";
        int pos = spaceName.indexOf(partitionPrefix);
        if (pos == -1)
            return -1;

        int routing = 0;
        pos = pos + partitionPrefix.length();
        for (char c = spaceName.charAt(pos++); c >= '0' && c <= '9'; c = spaceName.charAt(pos++))
            routing = routing * 10 + (c - '0');

        return routing - 1;
    }
}
