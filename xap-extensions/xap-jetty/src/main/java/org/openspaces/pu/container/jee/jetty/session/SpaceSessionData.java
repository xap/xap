/*
 * Copyright (c) 2008-2019, GigaSpaces Technologies, Inc. All Rights Reserved.
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
package org.openspaces.pu.container.jee.jetty.session;

import com.gigaspaces.internal.io.IOUtils;
import org.eclipse.jetty.server.session.SessionData;

import java.io.*;

/**
 * @since 15.0.0
 * @author Niv Ingberg
 */
public class SpaceSessionData extends SessionData implements Externalizable {
    private static final long serialVersionUID = 1L;

    /**
     * Required for @{@link Externalizable}
     */
    public SpaceSessionData() {
        super(null, null, null, 0, 0, 0, 0);
    }

    public SpaceSessionData(String id, String cpath, String vhost, long created, long accessed, long lastAccessed, long maxInactiveMs) {
        super(id, cpath, vhost, created, accessed, lastAccessed, maxInactiveMs);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(_id); //session id
        out.writeUTF(_contextPath); //context path
        out.writeUTF(_vhost); //first vhost
        out.writeLong(_accessed);//accessTime
        out.writeLong(_lastAccessed); //lastAccessTime
        out.writeLong(_created); //time created
        out.writeLong(_cookieSet);//time cookie was set
        out.writeUTF(_lastNode); //name of last node managing
        out.writeLong(_expiry);
        out.writeLong(_maxInactiveMs);
        IOUtils.writeMapStringObject(out, _attributes);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _id = in.readUTF();
        _contextPath = in.readUTF();
        _vhost = in.readUTF();
        _accessed = in.readLong();//accessTime
        _lastAccessed = in.readLong(); //lastAccessTime
        _created = in.readLong(); //time created
        _cookieSet = in.readLong();//time cookie was set
        _lastNode = in.readUTF(); //last managing node
        _expiry = in.readLong();
        _maxInactiveMs = in.readLong();
        _attributes = IOUtils.readMapStringObject(in);
    }
}
