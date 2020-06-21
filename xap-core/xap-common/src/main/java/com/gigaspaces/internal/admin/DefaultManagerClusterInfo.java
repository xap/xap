package com.gigaspaces.internal.admin;

import com.gigaspaces.internal.jvm.JVMDetails;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.start.manager.XapManagerClusterInfo;
import com.gigaspaces.start.manager.XapManagerConfig;
import com.gigaspaces.admin.ManagerClusterInfo;
import com.gigaspaces.admin.ManagerInstanceInfo;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * @author Niv Ingberg
 * @since 15.5
 */
public class DefaultManagerClusterInfo implements ManagerClusterInfo {
    private final List<ManagerInstanceInfo> managers;
    private final String zookeeperConnectionString;

    public DefaultManagerClusterInfo(JVMDetails jvmDetails) {
        this.managers = initManagers(jvmDetails);
        String zkPort = "2181"; // TODO: Configurable
        this.zookeeperConnectionString = initZookeeperConnectionString(managers, zkPort);
    }

    private static List<ManagerInstanceInfo> initManagers(JVMDetails jvmDetails) {
        String managerServers = GsEnv.get(XapManagerClusterInfo.SERVERS_ENV_VAR_SUFFIX, jvmDetails.getEnvironmentVariables());
        List<XapManagerConfig> xapManagerConfigs = XapManagerClusterInfo.parseServersEnvVar(managerServers);
        return Collections.unmodifiableList(xapManagerConfigs.stream()
                .map(DefaultManagerInstanceInfo::new)
                .collect(Collectors.toList()));
    }

    private static String initZookeeperConnectionString(Collection<ManagerInstanceInfo> managers, String port) {
        if (managers.isEmpty())
            return null;
        StringJoiner sj = new StringJoiner(",");
        for (ManagerInstanceInfo manager : managers) {
            sj.add(manager.getHost() + ":" + port);
        }
        return sj.toString();
    }

    @Override
    public List<ManagerInstanceInfo> getManagers() {
        return managers;
    }

    @Override
    public String getZookeeperConnectionString() {
        return zookeeperConnectionString;
    }
}
