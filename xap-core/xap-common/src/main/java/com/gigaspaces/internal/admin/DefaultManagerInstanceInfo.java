package com.gigaspaces.internal.admin;

import com.gigaspaces.start.manager.XapManagerConfig;
import com.gigaspaces.admin.ManagerInstanceInfo;

/**
 * @author Niv Ingberg
 * @since 15.5
 */
public class DefaultManagerInstanceInfo implements ManagerInstanceInfo {
    private final XapManagerConfig manager;

    public DefaultManagerInstanceInfo(XapManagerConfig manager) {
        this.manager = manager;
    }

    @Override
    public String getHost() {
        return manager.getHost();
    }
}
