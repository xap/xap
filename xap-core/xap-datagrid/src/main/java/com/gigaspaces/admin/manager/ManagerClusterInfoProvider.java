package com.gigaspaces.admin.manager;

import com.gigaspaces.admin.ManagerClusterInfo;

/**
 * @author Niv Ingberg
 * @since 15.5
 */
public interface ManagerClusterInfoProvider {
    ManagerClusterInfo getManagerClusterInfo();
}
