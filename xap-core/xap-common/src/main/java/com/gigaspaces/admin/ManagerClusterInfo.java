package com.gigaspaces.admin;

import java.io.Serializable;
import java.util.List;

/**
 * Encapsulates information about the cluster of managers in this grid
 *
 * @author Niv Ingberg
 * @since 15.5
 */
public interface ManagerClusterInfo extends Serializable {

    /**
     * Returns the manager cluster type.
     */
    ManagerClusterType getManagerClusterType();

    /**
     * Returns the list of manager instances (empty list if no managers).
     */
    List<ManagerInstanceInfo> getManagers();

    /**
     * Returns a connection string to the manager's zookeeper cluster.
     */
    String getZookeeperConnectionString();
}
