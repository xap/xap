package com.gigaspaces.admin;

import java.util.List;

/**
 * Encapsulates information about the cluster of managers in this grid
 *
 * @author Niv Ingberg
 * @since 15.5
 */
public interface ManagerClusterInfo {

    /**
     * Returns the list of manager instances (empty list if no managers).
     */
    List<ManagerInstanceInfo> getManagers();

    /**
     * Returns a connection string to the manager's zookeeper cluster.
     */
    String getZookeeperConnectionString();
}
