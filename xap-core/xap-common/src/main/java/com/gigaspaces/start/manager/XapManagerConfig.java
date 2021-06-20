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
import com.gigaspaces.admin.ManagerInstanceInfo;
import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Properties;

public class XapManagerConfig implements ManagerInstanceInfo, SmartExternalizable {
    private static final long serialVersionUID = 1L;

    // List of servers.
    // Each server has a host, and a map of component-to-port
    private String host;
    private Properties properties;

    private static final String DEFAULT_REST = GsEnv.property(CommonSystemProperties.MANAGER_REST_PORT).get("8090");
    private static final boolean SSL_ENABLED = Boolean.getBoolean(CommonSystemProperties.MANAGER_REST_SSL_ENABLED);

    // Required for Externalizable
    public XapManagerConfig() {
    }

    public XapManagerConfig(String host) {
        this(host, new Properties());
    }

    public XapManagerConfig(String host, Properties properties) {
        this.host = host;
        this.properties = properties;
    }

    public static XapManagerConfig parse(String s) {
        final String[] tokens = s.split(";");
        final String host = tokens[0];
        final Properties properties = new Properties();
        for (int i=1 ; i < tokens.length ; i++) {
            String token = tokens[i];
            int pos = token.indexOf('=');
            if (pos == -1)
                throw new IllegalArgumentException("Invalid manager config '" + s + "' - element '" + token + "' does not contain '='");
            String key = token.substring(0, pos);
            String value = token.substring(pos+1);
            properties.setProperty(key, value);
        }
        return new XapManagerConfig(host, properties);
    }

    @Override
    public String getHost() {
        return host;
    }

    public Properties getProperties() {
        return properties;
    }

    public String getZookeeper() {
        return properties.getProperty("zookeeper");
    }

    public String getLookupService() {
        return properties.getProperty("lus");
    }

    public String getAdminRest() {
        return properties.getProperty("rest", DEFAULT_REST);
    }

    public String getAdminRestUrl() {
        return getAdminRestUrl(getAdminRest());
    }

    public String getAdminRestUrl(String port) {
        return (SSL_ENABLED ? "https" : "http") + "://" + getHost() + ":" + port;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    @Override
    public String toString() {
        String result = host;
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            result += ";" + entry.getKey() + "=" + entry.getValue();
        }

        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        BootIOUtils.writeString(out, host);
        BootIOUtils.writeProperties(out, properties);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.host = BootIOUtils.readString(in);
        this.properties = BootIOUtils.readProperties(in);
    }
}
