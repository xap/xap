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


package com.j_spaces.core;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.security.service.SecurityContext;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * The SpaceContext class defines context info passed from the proxy to the space. In general, when
 * there is no context information, the context reference is null.
 **/

public class SpaceContext implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    // Security context holding authentication request or identification
    private SecurityContext securityContext;
    private int chunksMapGeneration;
    private boolean fromGateway;

    //token which is used to verify that the client has permissions to perform operations in quiesce mode
    private QuiesceToken quiesceToken;

    private boolean clustered;

    /**
     * Empty constructor for Externalizable impl.
     */
    public SpaceContext() {
    }

    public SpaceContext(SecurityContext securityContext) {
        this.securityContext = securityContext;
    }

    public SpaceContext(boolean fromGateway, int chunksMapGeneration, boolean clustered) {
        this.fromGateway = fromGateway;
        this.chunksMapGeneration = chunksMapGeneration;
        this.clustered = clustered;
    }

    public SpaceContext createCopy(SecurityContext newSecurityContext) {
        SpaceContext newContext = new SpaceContext(fromGateway, chunksMapGeneration, clustered);
        newContext.securityContext = newSecurityContext;
        return newContext;
    }

    public SecurityContext getSecurityContext() {
        return securityContext;
    }

    public int getChunksMapGeneration() {
        return chunksMapGeneration;
    }

    public boolean isFromGateway() {
        return fromGateway;
    }

    public boolean isClustered(){ return clustered;}

    private static final short FLAG_SECURITY = 1 << 0;
    private static final short FLAG_FROM_GATEWAY = 1 << 1;
    private static final short FLAG_QUIESCE_TOKEN = 1 << 2;
    private static final short FLAG_CLUSTERED = 1 << 3;

    public void writeExternal(ObjectOutput out) throws IOException {
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if(version.greaterOrEquals(PlatformLogicalVersion.v15_5_0))
            writeExternalV15_5_0(out);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v10_1_0))
            writeExternalV10_1_0(out);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v9_7_0))
            writeExternalV9_7_0(out);
        else
            writeExternalV8_0_3(out);
    }

    private void writeExternalV15_5_0(ObjectOutput out) throws IOException {
        final short flags = buildFlags();
        out.writeShort(flags);
        if (securityContext != null)
            IOUtils.writeObject(out, securityContext);
        if (quiesceToken != null)
            IOUtils.writeObject(out, quiesceToken);
        out.writeShort(((short) this.chunksMapGeneration));
    }

    private void writeExternalV10_1_0(ObjectOutput out) throws IOException {
        final short flags = buildFlags();
        out.writeShort(flags);
        if (securityContext != null)
            IOUtils.writeObject(out, securityContext);
        if (quiesceToken != null)
            IOUtils.writeObject(out, quiesceToken);
    }

    private void writeExternalV9_7_0(ObjectOutput out) throws IOException {
        final short flags = buildFlags();
        out.writeShort(flags);
        if (securityContext != null)
            IOUtils.writeObject(out, securityContext);
    }

    private void writeExternalV8_0_3(ObjectOutput out) throws IOException {
        if (securityContext != null) {
            out.writeBoolean(true);
            out.writeObject(securityContext);
        } else
            out.writeBoolean(false);

        out.writeBoolean(fromGateway);
        if (fromGateway)
            out.writeBoolean(true);
    }

    private short buildFlags() {
        short flags = 0;

        if (securityContext != null)
            flags |= FLAG_SECURITY;
        if (fromGateway)
            flags |= FLAG_FROM_GATEWAY;
        if (quiesceToken != null)
            flags |= FLAG_QUIESCE_TOKEN;
        if(clustered)
            flags |= FLAG_CLUSTERED;

        return flags;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v15_8_0))
            readExternalV15_8_0(in);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v15_5_0))
            readExternalV15_5_0(in);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v10_1_0))
            readExternalV10_1_0(in);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v9_7_0))
            readExternalV9_7_0(in);
        else
            readExternalV8_0_3(in);
    }

    private void readExternalV15_8_0(ObjectInput in) throws IOException, ClassNotFoundException {
        short flags = in.readShort();
        this.fromGateway = (flags & FLAG_FROM_GATEWAY) != 0;
        this.clustered = (flags & FLAG_CLUSTERED) != 0;

        if ((flags & FLAG_SECURITY) != 0)
            securityContext = IOUtils.readObject(in);

        if ((flags & FLAG_QUIESCE_TOKEN) != 0)
            quiesceToken = IOUtils.readObject(in);

        this.chunksMapGeneration = in.readShort();

    }

    private void readExternalV15_5_0(ObjectInput in) throws IOException, ClassNotFoundException {
        short flags = in.readShort();
        this.fromGateway = (flags & FLAG_FROM_GATEWAY) != 0;

        if ((flags & FLAG_SECURITY) != 0)
            securityContext = IOUtils.readObject(in);

        if ((flags & FLAG_QUIESCE_TOKEN) != 0)
            quiesceToken = IOUtils.readObject(in);

        this.chunksMapGeneration = ((int) in.readShort());
    }

    private void readExternalV10_1_0(ObjectInput in) throws IOException, ClassNotFoundException {
        short flags = in.readShort();
        this.fromGateway = (flags & FLAG_FROM_GATEWAY) != 0;

        if ((flags & FLAG_SECURITY) != 0)
            securityContext = IOUtils.readObject(in);

        if ((flags & FLAG_QUIESCE_TOKEN) != 0)
            quiesceToken = IOUtils.readObject(in);
    }

    private void readExternalV9_7_0(ObjectInput in) throws IOException, ClassNotFoundException {
        short flags = in.readShort();
        this.fromGateway = (flags & FLAG_FROM_GATEWAY) != 0;

        if ((flags & FLAG_SECURITY) != 0)
            securityContext = IOUtils.readObject(in);
    }

    private void readExternalV8_0_3(ObjectInput in) throws IOException, ClassNotFoundException {
        if (in.readBoolean())
            securityContext = (SecurityContext) in.readObject();
        this.fromGateway = in.readBoolean();
        if (fromGateway) {
            // For backwards.
            in.readBoolean();
        }
    }

    public QuiesceToken getQuiesceToken() {
        return quiesceToken;
    }

    public void setQuiesceToken(QuiesceToken quiesceToken) {
        this.quiesceToken = quiesceToken;
    }
}
