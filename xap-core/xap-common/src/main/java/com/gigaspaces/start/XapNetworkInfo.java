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
package com.gigaspaces.start;

import com.gigaspaces.internal.utils.GsEnv;
import org.jini.rio.boot.BootUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Niv Ingberg
 * @since 12.1
 */
public class XapNetworkInfo {
    private static volatile XapNetworkInfo instance;

    private final String hostId;
    private final InetAddress host;
    private final InetAddress publicHost;
    private String publicHostId;
    private boolean publicHostConfigured;

    public static XapNetworkInfo getInstance() {
        XapNetworkInfo snapshot = instance;
        if (snapshot != null)
            return snapshot;
        synchronized (XapNetworkInfo.class) {
            if (instance == null)
                instance = new XapNetworkInfo();
            return instance;
        }
    }

    private XapNetworkInfo() {
        try {
            this.hostId = BootUtil.getHostAddress();
            this.host = InetAddress.getByName(hostId);

            //apply public host if configured, otherwise same as bind address
            publicHostId = GsEnv.get("PUBLIC_HOST");
            if (publicHostId == null) {
                publicHostId = hostId;
                publicHost = host;
            } else {
                publicHostConfigured = true;
                publicHost = InetAddress.getByName(publicHostId);
            }

        } catch (UnknownHostException e) {
            throw new IllegalStateException("Failed to get network information", e);
        }
    }

    public String getHostId() {
        return hostId;
    }

    public InetAddress getHost() {
        return host;
    }


    public String getPublicHostId() {
        return publicHostId;
    }

    public InetAddress getPublicHost() {
        return publicHost;
    }


    public boolean isPublicHostConfigured(){
        return publicHostConfigured;
    }
}
