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
package com.gigaspaces.start.manager;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.admin.ManagerClusterInfo;
import com.gigaspaces.admin.ManagerClusterType;
import com.gigaspaces.admin.ManagerInstanceInfo;
import com.gigaspaces.grid.zookeeper.ZookeeperConfig;
import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.serialization.SmartExternalizable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.util.IPAddressUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.util.*;

public class XapManagerClusterInfo implements ManagerClusterInfo, SmartExternalizable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_MANAGER);

    public static final String SERVERS_PROPERTY = "com.gs.manager.servers";
    public static final String SERVER_PROPERTY = "com.gs.manager.server";
    public static final String SERVERS_ENV_VAR_SUFFIX = "MANAGER_SERVERS";
    public static final String SERVERS_ENV_VAR = "GS_" + SERVERS_ENV_VAR_SUFFIX;
    public static final String SERVER_ENV_VAR_SUFFIX = "MANAGER_SERVER";
    private static final String MANAGER_CLUSTER_TYPE_ENV_VAR_SUFFIX = "MANAGER_CLUSTER_TYPE";
    public static final String MANAGER_CLUSTER_TYPE_ENV_VAR = "GS_"+MANAGER_CLUSTER_TYPE_ENV_VAR_SUFFIX;

    private transient XapManagerConfig currServer;
    private List<XapManagerConfig> servers;
    private List<String> hosts;
    private ManagerClusterType managerClusterType;
    private transient String zookeeperConnectionString;

    // Required for externalizable
    public XapManagerClusterInfo() {
    }

    public XapManagerClusterInfo(InetAddress currHost) {
        this(System.getProperties(), System.getenv(), currHost);
    }

    public XapManagerClusterInfo(String host, InetAddress currHost) {
        this(parseManagerClusterType(System.getenv()), Collections.singletonList(XapManagerConfig.parse(host)), currHost);
    }

    public XapManagerClusterInfo(Properties sysProps, Map<String, String> env) {
        this(sysProps, env, null);
    }

    private XapManagerClusterInfo(Properties sysProps, Map<String, String> env, InetAddress currHost) {
        this(parseManagerClusterType(env), parse(sysProps, env), currHost);
    }

    private XapManagerClusterInfo(ManagerClusterType managerClusterType, List<XapManagerConfig> servers, InetAddress currHost) {
        this.managerClusterType = managerClusterType;
        if (servers.size() != 0 && servers.size() != 1 && servers.size() != 3)
            throw new UnsupportedOperationException("Unsupported xap manager cluster size: " + servers.size());
        this.servers = Collections.unmodifiableList(servers);
        this.hosts = initHosts(servers);
        this.currServer = findManagerByHost(currHost);
        if (currServer != null) {
            System.setProperty(CommonSystemProperties.MANAGER_REST_URL, currServer.getAdminRestUrl());
        }
    }

    private static List<String> initHosts(List<XapManagerConfig> servers) {
        if (servers.isEmpty())
            return Collections.emptyList();
        List<String> hosts = new ArrayList<>(servers.size());
        servers.forEach(s -> hosts.add(s.getHost()));
        return Collections.unmodifiableList(hosts);
    }

    @Override
    public ManagerClusterType getManagerClusterType() {
        return managerClusterType;
    }

    public List<XapManagerConfig> getServers() {
        return servers;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public boolean isEmpty() {
        return servers.isEmpty();
    }

    public int size() {
        return servers.size();
    }

    public XapManagerConfig getCurrServer() {
        return currServer;
    }

    @Override
    public String toString() {
        return Arrays.toString(servers.toArray());
    }

    private static ManagerClusterType parseManagerClusterType(Map<String, String> env) {
        return ManagerClusterType.valueOf(GsEnv.get(MANAGER_CLUSTER_TYPE_ENV_VAR_SUFFIX, ManagerClusterType.SERVICE_GRID.name(), env));
    }

    private static List<XapManagerConfig> parse(Properties sysProps, Map<String, String> env) {
        final List<XapManagerConfig> shortList = parseShort(sysProps, env);
        final List<XapManagerConfig> fullList = parseFull(sysProps, env);
        if (shortList.size() != 0 && fullList.size() != 0)
            throw new IllegalStateException("Ambiguous XAP manager cluster configuration (short and full)");
        return shortList.size() != 0 ? shortList : fullList;
    }

    private static List<XapManagerConfig> parseShort(Properties sysProps, Map<String, String> env) {
        final String var = get(SERVERS_PROPERTY, SERVERS_ENV_VAR_SUFFIX, sysProps, env);
        final List<XapManagerConfig> result = new ArrayList<XapManagerConfig>();
        if (var != null && !var.isEmpty()) {
            final String[] tokens = var.split(",");
            for (String token : tokens) {
                result.add(XapManagerConfig.parse(token));
            }
        }
        return result;
    }

    private static List<XapManagerConfig> parseFull(Properties sysProps, Map<String, String> env) {
        final List<XapManagerConfig> result = new ArrayList<XapManagerConfig>();
        for (int i=1 ; i < 10 ; i++) {
            final String var = get(SERVER_PROPERTY + "." + i, SERVER_ENV_VAR_SUFFIX + "_" + i, sysProps, env);
            if (var != null && var.length() != 0)
                result.add(parse(var));
            else
                break;
        }
        return result;
    }

    private static XapManagerConfig parse(String s) {
        XapManagerConfig result = XapManagerConfig.parse(s);
        if (logger.isDebugEnabled())
            logger.debug("Parse XapManagerConfig " + result);
        return result;
    }

    private static String get(String sysProp, String envVarSuffix, Properties sysProps, Map<String, String> env) {
        String result = sysProps.getProperty(sysProp);
        if (result != null) {
            if (logger.isDebugEnabled())
                logger.debug("Loaded config from system property " + sysProp + "=" + result);
            return result;
        }
        String envVar = GsEnv.key(envVarSuffix, env);
        if (envVar != null) {
            result = env.get(envVar);
            if (logger.isDebugEnabled())
                logger.debug("Loaded config from environment variable " + envVar + "=" + result);
            return result;
        }
        return null;
    }

    private XapManagerConfig findManagerByHost(InetAddress currHost) {
        if (currHost == null)
            return null;
        XapManagerConfig result = null;
        for (XapManagerConfig server : servers) {
            if (server.getHost().equals(currHost.getHostName()) ||
                    server.getHost().equals(currHost.getHostAddress()) ||
                    Arrays.equals(tryParseIpv6(server.getHost()), currHost.getAddress())) {
                result = server;
            }
        }
        if (result == null && servers.size() == 1) {
            if (servers.get(0).getHost().equals("localhost") || servers.get(0).getHost().equals("127.0.0.1")){
                result = servers.get(0);
            }
        }
        if (logger.isDebugEnabled()) {
            if (result == null)
                logger.debug("Current host [" + currHost +"] is not part of configured managers");
            else
                logger.debug("Current manager is " + result);
        }
        return result;
    }

    private static byte[] tryParseIpv6(String s) {
        if (s.startsWith("[") && s.endsWith("]")) {
            s = s.substring(1, s.length()-1);
        }
        return IPAddressUtil.textToNumericFormatV6(s);
    }

    @Override
    public List<ManagerInstanceInfo> getManagers() {
        return (List)servers;
    }

    @Override
    public String getZookeeperConnectionString() {
        if (zookeeperConnectionString == null) {
            zookeeperConnectionString = initZookeeperConnectionString(getManagers(), ZookeeperConfig.getDefaultClientPort());
        }
        return zookeeperConnectionString;
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
    public void writeExternal(ObjectOutput out) throws IOException {
        BootIOUtils.writeString(out, managerClusterType.name());
        out.writeInt(servers.size());
        for (XapManagerConfig server : servers) {
            server.writeExternal(out);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.managerClusterType = Enum.valueOf(ManagerClusterType.class, BootIOUtils.readString(in));
        int numOfServers = in.readInt();
        List<XapManagerConfig> servers = new ArrayList<>(numOfServers);
        for (int i = 0; i < numOfServers; i++) {
            XapManagerConfig server = new XapManagerConfig();
            server.readExternal(in);
            servers.add(server);
        }
        this.servers = Collections.unmodifiableList(servers);
        this.hosts = initHosts(this.servers);
    }
}
