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


package org.openspaces.jee.sessions.jetty;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceProperty;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.query.IdQuery;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The entry that is stored in the Space representing a Jetty session.
 */
@SpaceClass
public class SessionData implements Externalizable {

    private static final long serialVersionUID = 3104738310898353395L;

    private String _id;
    private long _accessed = -1;
    private volatile long _lastAccessed = -1;
    private volatile long _lastSaved = -1;
    private long _maxIdleMs = -1;
    private volatile long _cookieSet = -1;
    private long _created = -1;
    private volatile long _expiryTime = -1;
    private volatile Map<String, Object> _attributes = null;


    public SessionData() {
    }

    public SessionData(String sessionId) {
        _id = sessionId;
        _created = System.currentTimeMillis();
        _accessed = _created;
        _lastAccessed = 0;
        _lastSaved = 0;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        if (_id == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(_id);
        }
        out.writeLong(_accessed);
        out.writeLong(_lastAccessed);
        out.writeLong(_lastSaved);
        out.writeLong(_maxIdleMs);
        out.writeLong(_cookieSet);
        out.writeLong(_created);
        out.writeLong(_expiryTime);
        if (_attributes == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(_attributes.size());
            for (Iterator<Map.Entry<String, Object>> it = _attributes.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Object> entry = it.next();
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if (in.readBoolean()) {
            _id = in.readUTF();
        }
        _accessed = in.readLong();
        _lastAccessed = in.readLong();
        _lastSaved = in.readLong();
        _maxIdleMs = in.readLong();
        _cookieSet = in.readLong();
        _created = in.readLong();
        _expiryTime = in.readLong();
        int size = in.readInt();
        if (size > -1) {
            _attributes = new ConcurrentHashMap<String, Object>();
            for (int i = 0; i < size; i++) {
                _attributes.put((String) in.readObject(), in.readObject());
            }
        }
    }

    @SpaceId(autoGenerate = false)
    @SpaceRouting
    public String getId() {
        return _id;
    }

    public void setId(String id) {
        _id = id;
    }

    @SpaceProperty(nullValue = "-1")
    public long getCreated() {
        return _created;
    }

    public void setCreated(long ms) {
        _created = ms;
    }

    @SpaceProperty(nullValue = "-1")
    public long getAccessed() {
        return _accessed;
    }

    public void setAccessed(long ms) {
        _accessed = ms;
    }

    public void setLastSaved(long ms) {
        _lastSaved = ms;
    }

    @SpaceProperty(nullValue = "-1")
    public long getLastSaved() {
        return _lastSaved;
    }

    public void setMaxIdleMs(long ms) {
        _maxIdleMs = ms;
    }

    @SpaceProperty(nullValue = "-1")
    public long getMaxIdleMs() {
        return _maxIdleMs;
    }

    public void setLastAccessed(long ms) {
        _lastAccessed = ms;
    }

    @SpaceProperty(nullValue = "-1")
    public long getLastAccessed() {
        return _lastAccessed;
    }

    public void setCookieSet(long ms) {
        _cookieSet = ms;
    }

    @SpaceProperty(nullValue = "-1")
    public long getCookieSet() {
        return _cookieSet;
    }

    @SpaceProperty
    protected Map<String, Object> getAttributeMap() {
        return _attributes;
    }

    protected void setAttributeMap(Map<String, Object> map) {
        _attributes = map;
    }

    public void setExpiryTime(long time) {
        _expiryTime = time;
    }

    @SpaceProperty(nullValue = "-1")
    public long getExpiryTime() {
        return _expiryTime;
    }

    public String toString() {
        return "Session id=" + _id +
                ",created=" + _created + ",accessed=" + _accessed +
                ",lastAccessed=" + _lastAccessed +
                ",cookieSet=" + _cookieSet +
                ",expiryTime=" + _expiryTime;
    }

    public String toStringExtended() {
        return toString() + "values=" + _attributes;
    }

    public static IdQuery<SessionData> newIdQuery(String id) {
        return new IdQuery<SessionData>(SessionData.class, id);
    }

}
