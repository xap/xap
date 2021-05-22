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

package com.gigaspaces.lrmi.nio.info;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class NIODetails implements SmartExternalizable {
    private static final long serialVersionUID = -4614637448976120646L;

    private long id;
    private String hostAddress = "";
    private String hostName = "";
    private String bindHost = "";
    private int port = -1;
    private int minThreads = -1;
    private int maxThreads = -1;
    private boolean sslEnabled;

    public NIODetails() {
    }

    public NIODetails(long id, String hostAddress, String hostName, String bindHost, int port, int minThreads, int maxThreads,
                      boolean sslEnabled) {
        this.id = id;
        this.hostAddress = hostAddress;
        this.hostName = hostName;
        this.bindHost = bindHost;
        this.minThreads = minThreads;
        this.maxThreads = maxThreads;
        this.port = port;
        this.sslEnabled = sslEnabled;
    }

    public long getId() {
        return id;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public String getHostName() {
        return hostName;
    }

    public String getBindHost() {
        return bindHost;
    }

    public int getPort() {
        return port;
    }

    public int getMinThreads() {
        return minThreads;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(id);
        out.writeUTF(bindHost);
        out.writeUTF(hostAddress);
        out.writeUTF(hostName);
        out.writeInt(port);
        out.writeInt(minThreads);
        out.writeInt(maxThreads);
        out.writeBoolean(sslEnabled);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readLong();
        bindHost = in.readUTF();
        hostAddress = in.readUTF();
        hostName = in.readUTF();
        port = in.readInt();
        minThreads = in.readInt();
        maxThreads = in.readInt();
        sslEnabled = in.readBoolean();
    }

    @Override
    public String toString() {
        return "NIODetails [getId()=" + getId() + ", getHostAddress()=" + getHostAddress() + ", getHostName()="
                + getHostName() + ", getBindHost()=" + getBindHost() + ", getPort()=" + getPort()
                + ", getMinThreads()=" + getMinThreads() + ", getMaxThreads()=" + getMaxThreads() + ", isSslEnabled()="
                + isSslEnabled() + "]";
    }


}
