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

package com.gigaspaces.lrmi;

import com.gigaspaces.logger.Constants;
import com.j_spaces.kernel.SystemProperties;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A default network mapper based on a configuration file named "network_mapping" simply maps a
 * given address to the other as specified in the configuration file
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class DefaultNetworkMapper
        implements INetworkMapper {
    final private static Logger _logger = Logger.getLogger(Constants.LOGGER_LRMI);

    private static final String NETWORK_MAPPING_FILE = System.getProperty(SystemProperties.LRMI_NETWORK_MAPPING_FILE, "config/network_mapping.config");
    private static final String MALFORMED_FORMAT_MSG = "Unsupported format of network mapping file, " +
            "expected format is separated lines each contains a separate mapping: " +
            "<original ip>:<original port>,<mapped ip>:<mapped port> for instance 10.0.0.1:4162,212.321.1.1:3000";
    private final HashMap<ServerAddress, ServerAddress> _mapping = new HashMap<ServerAddress, ServerAddress>();


    public DefaultNetworkMapper() {
        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(NETWORK_MAPPING_FILE);
        if (resourceAsStream == null) {
            if (_logger.isLoggable(Level.INFO))
                _logger.info(" file " + NETWORK_MAPPING_FILE + " in the classpath, no mapping created");
            return;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAsStream));
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                //Rermarked line
                if (line.startsWith("#"))
                    continue;
                String[] split = line.split(",");
                if (split.length != 2)
                    throw new IllegalArgumentException(MALFORMED_FORMAT_MSG);
                ServerAddress original = getAddress(split[0].trim());
                ServerAddress mapped = getAddress(split[1].trim());
                if (_mapping.containsKey(original))
                    throw new IllegalArgumentException("Address " + original + " is already mapped to " + _mapping.get(original));
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine("Adding mapping of " + original + " to " + mapped);
                _mapping.put(original, mapped);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error while parsing the network mapping file " + NETWORK_MAPPING_FILE, e);
        }
    }


    private ServerAddress getAddress(String string) {
        String[] split = string.split(":");
        if (split.length != 2)
            throw new IllegalArgumentException(MALFORMED_FORMAT_MSG);

        try {
            return new ServerAddress(split[0], Integer.parseInt(split[1]));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(MALFORMED_FORMAT_MSG);
        }
    }


    @Override
    public ServerAddress mapHost(ServerAddress serverAddress) {
        _logger.warning(  "request to mapHost " + serverAddress);
        for (ServerAddress from:  _mapping.keySet()){
            if (serverAddress.getHost().equalsIgnoreCase(from.getHost())){
                String mapped = _mapping.get(from).getHost();
                ServerAddress res = new ServerAddress(mapped, serverAddress.getPort());
                _logger.warning("hostMap from " + serverAddress + " to " + res);
                return res;
            }
            _logger.warning("hostMap no mapping" + serverAddress);
        }
        return serverAddress;
    }

    @Override
    public ServerAddress map(ServerAddress serverAddress) {
        _logger.warning("enter to map with param "+ serverAddress);
        _logger.warning("********************printing map details**************************");
        for (ServerAddress from:  _mapping.keySet()){
            ServerAddress to = _mapping.get(from);
            _logger.warning("from: "+ from + ", to: "+ to);
        }
        _logger.warning("********************end printing map details**************************");

        ServerAddress transformed = _mapping.get(serverAddress);
        //No mapping, return original
        if (transformed == null) {
            if (_logger.isLoggable(Level.FINEST)) {
                _logger.warning("No mapping exists for provided address " + serverAddress + " returning original address");
            }
            return serverAddress;
        }
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.warning("Mapping  address " + serverAddress + " to " + transformed);
        }
        _logger.warning("enter to map with server address "+ serverAddress +", to " +  transformed);
        return transformed;
    }

}
