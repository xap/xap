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


package com.gigaspaces.security.audit;

import com.gigaspaces.security.service.SecurityContext;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.start.SystemInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;

/**
 * The <code>AuditDetails</code> are client side auditing details sent to the server on each
 * authentication request. These details are part of the {@link SecurityContext} instance. <p> The
 * audit details are available when using Space filters. See for example the "before-authentication"
 * filter operation code, or the <code>@BeforeAuthentication</code> annotation of OpenSpaces
 * filters. <p> There is no need to have a Space Filter for auditing. These details are part of the
 * {@link com.gigaspaces.security.session.SessionDetails} which is supplied to the {@link
 * SecurityAudit} implementation.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class AuditDetails implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    /**
     * remote host name/ip
     */
    private String host;

    /**
     * Constructs the session details - done at proxy side.
     */
    public AuditDetails() {
        InetAddress localHost = SystemInfo.singleton().network().getHost();
        host = localHost.getHostName() + "/" + localHost.getHostAddress();
    }

    /**
     * @return the remote host name/ip
     */
    public String getHost() {
        return host;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        host = in.readUTF();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(host);
    }
}
