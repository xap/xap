package com.gigaspaces.start.manager;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.admin.ManagerClusterType;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.logger.Constants;

import java.net.InetAddress;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.util.IPAddressUtil;

public class XapManagerClusterInfo {
    private static final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_MANAGER);

    public static final String SERVERS_PROPERTY = "com.gs.manager.servers";
    public static final String SERVER_PROPERTY = "com.gs.manager.server";
    public static final String SERVERS_ENV_VAR_SUFFIX = "MANAGER_SERVERS";
    public static final String SERVERS_ENV_VAR = "GS_" + SERVERS_ENV_VAR_SUFFIX;
    public static final String SERVER_ENV_VAR_SUFFIX = "MANAGER_SERVER";
    private static final String MANAGER_CLUSTER_TYPE_ENV_VAR_SUFFIX = "MANAGER_CLUSTER_TYPE";
    public static final String MANAGER_CLUSTER_TYPE_ENV_VAR = "GS_"+MANAGER_CLUSTER_TYPE_ENV_VAR_SUFFIX;

    private final XapManagerConfig currServer;
    private final XapManagerConfig[] servers;
    private final ManagerClusterType managerClusterType;

    public XapManagerClusterInfo(InetAddress currHost) {
        this(parse(), currHost);
    }

    public XapManagerClusterInfo(String host, InetAddress currHost) {
        this(Collections.singletonList(XapManagerConfig.parse(host)), currHost);
    }

    private XapManagerClusterInfo(Collection<XapManagerConfig> servers, InetAddress currHost) {
        if (servers.size() != 0 && servers.size() != 1 && servers.size() != 3)
            throw new UnsupportedOperationException("Unsupported xap manager cluster size: " + servers.size());
        this.servers = servers.toArray(new XapManagerConfig[servers.size()]);
        this.currServer = findManagerByHost(currHost);
        if (currServer != null) {
            System.setProperty(CommonSystemProperties.MANAGER_REST_URL, currServer.getAdminRestUrl());
        }

        this.managerClusterType = GsEnv.getOptional(MANAGER_CLUSTER_TYPE_ENV_VAR_SUFFIX).map(ManagerClusterType::valueOf).orElse(ManagerClusterType.SERVICE_GRID);
    }

    public ManagerClusterType getManagerClusterType() {
        return managerClusterType;
    }

    public XapManagerConfig[] getServers() {
        return servers;
    }

    public boolean isEmpty() {
        return servers.length == 0;
    }

    public XapManagerConfig getCurrServer() {
        return currServer;
    }

    @Override
    public String toString() {
        return Arrays.toString(servers);
    }

    private static Collection<XapManagerConfig> parse() {
        final Collection<XapManagerConfig> shortList = parseShort();
        final Collection<XapManagerConfig> fullList = parseFull();
        if (shortList.size() != 0 && fullList.size() != 0)
            throw new IllegalStateException("Ambiguous XAP manager cluster configuration (short and full)");
        return shortList.size() != 0 ? shortList : fullList;
    }

    private static Collection<XapManagerConfig> parseShort() {
        final String var = get(SERVERS_PROPERTY, SERVERS_ENV_VAR_SUFFIX);
        return parseServersEnvVar( var );
    }

    public static List<XapManagerConfig> parseServersEnvVar( String serversEnvVar ) {
        final List<XapManagerConfig> result = new ArrayList<XapManagerConfig>();
        if (serversEnvVar != null && !serversEnvVar.isEmpty()) {
            final String[] tokens = serversEnvVar.split(",");
            for (String token : tokens) {
                result.add(XapManagerConfig.parse(token));
            }
        }
        return result;
    }

    private static Collection<XapManagerConfig> parseFull() {
        final Collection<XapManagerConfig> result = new ArrayList<XapManagerConfig>();
        for (int i=1 ; i < 10 ; i++) {
            final String var = get(SERVER_PROPERTY + "." + i, SERVER_ENV_VAR_SUFFIX + "_" + i);
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

    private static String get(String sysProp, String envVarSuffix) {
        String result = System.getProperty(sysProp);
        if (result != null) {
            if (logger.isDebugEnabled())
                logger.debug("Loaded config from system property " + sysProp + "=" + result);
            return result;
        }
        String envVar = GsEnv.key(envVarSuffix);
        if (envVar != null) {
            result = System.getenv(envVar);
            if (logger.isDebugEnabled())
                logger.debug("Loaded config from environment variable " + envVar + "=" + result);
            return result;
        }
        return null;
    }

    private XapManagerConfig findManagerByHost(InetAddress currHost) {
        XapManagerConfig result = null;
        for (XapManagerConfig server : servers) {
            if (server.getHost().equals(currHost.getHostName()) ||
                    server.getHost().equals(currHost.getHostAddress()) ||
                    Arrays.equals(tryParseIpv6(server.getHost()), currHost.getAddress())) {
                result = server;
            }
        }
        if (result == null && servers.length == 1) {
            if (servers[0].getHost().equals("localhost") || servers[0].getHost().equals("127.0.0.1")){
                result = servers[0];
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
}
