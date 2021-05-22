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

//Internal Doc
package com.gigaspaces.security;

import com.gigaspaces.security.session.SessionId;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * An Authentication token which is used to further interact without using sensitive authentication
 * details (i.e. password). Tokens are usually cached and leased for a specific time period.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class AuthenticationToken implements SmartExternalizable {
    private static final long serialVersionUID = 1L;
    private SessionId sessionId;

    /**
     * Empty construct
     */
    public AuthenticationToken() {
    }

    /**
     * Constructs an authentication token with a random session id.
     */
    public AuthenticationToken(SessionId sessionId) {
        this.sessionId = sessionId;
    }

    /*
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((sessionId == null) ? 0 : sessionId.hashCode());
        return result;
    }

    /*
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AuthenticationToken other = (AuthenticationToken) obj;
        if (sessionId == null) {
            if (other.sessionId != null)
                return false;
        } else if (!sessionId.equals(other.sessionId))
            return false;
        return true;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sessionId = new SessionId();
        sessionId.readExternal(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        sessionId.writeExternal(out);
    }
}
