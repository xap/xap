package com.gigaspaces.internal.server.space;

import com.gigaspaces.internal.cluster.SpaceClusterInfo;

public interface IClusterInfoChangedListener {


    /***
     * Updates server side components that holds SpaceClusterInfo instances when SpaceClusterInfo changes (i.e horizontal scale event)
     * @param clusterInfo
     */
    void afterClusterInfoChange(SpaceClusterInfo clusterInfo);
}
