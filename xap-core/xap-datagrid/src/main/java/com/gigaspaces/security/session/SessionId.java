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
package com.gigaspaces.security.session;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

/**
 * Represents a unique identifier for an established session.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class SessionId implements SmartExternalizable {

    private static final long serialVersionUID = 1L;
    private String identifier;

    /**
     * Constructs an empty session Id.
     */
    public SessionId() {
    }

    /**
     * Generate a session Id to be used throughout the established session. The Id is generated
     * using a cryptographically strong pseudo random number generator.
     */
    public static SessionId generateIdentifier() {
        SessionId sessionId = new SessionId();
        sessionId.identifier = String.valueOf(System.currentTimeMillis()).concat("-").concat(UUID.randomUUID().toString());
        return sessionId;
    }

    /*
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((identifier == null) ? 0 : identifier.hashCode());
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
        SessionId other = (SessionId) obj;
        if (identifier == null) {
            if (other.identifier != null)
                return false;
        } else if (!identifier.equals(other.identifier))
            return false;
        return true;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        identifier = in.readUTF();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(identifier);
    }
}
