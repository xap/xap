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
import com.gigaspaces.logger.Constants;

import java.net.InetAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class XapManagerClusterInfo {
    private static final Logger logger = Logger.getLogger(Constants.LOGGER_MANAGER);

    public static final String SERVERS_PROPERTY = "com.gs.manager.servers";
    public static final String SERVER_PROPERTY = "com.gs.manager.server";
    public static final String SERVERS_ENV_VAR = "XAP_MANAGER_SERVERS";
    public static final String SERVER_ENV_VAR = "XAP_MANAGER_SERVER";

    private final XapManagerConfig currServer;
    private final XapManagerConfig[] servers;

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
        final String var = get(SERVERS_PROPERTY, SERVERS_ENV_VAR);
        return parseServersEnvVar( var );
    }

    public static Collection<XapManagerConfig> parseServersEnvVar( String serversEnvVar ) {
        final Collection<XapManagerConfig> result = new ArrayList<XapManagerConfig>();
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
            final String var = get(SERVER_PROPERTY + "." + i, SERVER_ENV_VAR + "_" + i);
            if (var != null && var.length() != 0)
                result.add(parse(var));
            else
                break;
        }
        return result;
    }

    private static XapManagerConfig parse(String s) {
        XapManagerConfig result = XapManagerConfig.parse(s);
        if (logger.isLoggable(Level.CONFIG))
            logger.log(Level.CONFIG, "Parse XapManagerConfig " + result);
        return result;
    }

    private static String get(String sysProp, String envVar) {
        String result = System.getProperty(sysProp);
        if (result != null) {
            if (logger.isLoggable(Level.CONFIG))
                logger.log(Level.CONFIG, "Loaded config from system property " + sysProp + "=" + result);
            return result;
        }
        result = System.getenv(envVar);
        if (result != null) {
            if (logger.isLoggable(Level.CONFIG))
                logger.log(Level.CONFIG, "Loaded config from environment variable " + envVar + "=" + result);
            return result;
        }
        return null;
    }

    private XapManagerConfig findManagerByHost(InetAddress currHost) {
        XapManagerConfig result = null;
        for (XapManagerConfig server : servers) {
            if (server.getHost().equals(currHost.getHostName()) || server.getHost().equals(currHost.getHostAddress())){
                result = server;
            }
        }
        if (result == null && servers.length == 1) {
            if (servers[0].getHost().equals("localhost") || servers[0].getHost().equals("127.0.0.1")){
                result = servers[0];
            }
        }
        if (logger.isLoggable(Level.CONFIG)) {
            if (result == null)
                logger.log(Level.CONFIG, "Current host [" + currHost +"] is not part of configured managers");
            else
                logger.log(Level.CONFIG, "Current manager is " + result);
        }
        return result;
    }
}
