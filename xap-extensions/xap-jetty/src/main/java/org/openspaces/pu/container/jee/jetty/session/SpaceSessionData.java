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

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import org.eclipse.jetty.server.session.SessionData;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Port of Jetty's SessionData class adapted for space.
 * @since 15.0.0
 * @author Niv Ingberg
 */
public class SpaceSessionData {
    private String spaceId;
    private String _id;
    private String _contextPath;
    private String _vhost;
    private String _lastNode;
    private long _expiry; //precalculated time of expiry in ms since epoch
    private long _created;
    private long _cookieSet;
    private long _accessed;         // the time of the last access
    private long _lastAccessed;     // the time of the last access excluding this one
    private long _maxInactiveMs;
    private Map<String,Object> _attributes;

    public SpaceSessionData() {
    }

    SpaceSessionData(String spaceId, SessionData sd) {
        this(spaceId, sd.getId(), sd.getContextPath(), sd.getVhost(), sd.getCreated(), sd.getAccessed(), sd.getLastAccessed(), sd.getMaxInactiveMs(), sd.getAllAttributes());
    }

    public SpaceSessionData(String spaceId, String id, String cpath, String vhost, long created, long accessed, long lastAccessed, long maxInactiveMs, Map<String,Object> attributes) {
        this.spaceId = spaceId;
        _id = id;
        setContextPath(cpath);
        setVhost(vhost);
        _created = created;
        _accessed = accessed;
        _lastAccessed = lastAccessed;
        _maxInactiveMs = maxInactiveMs;
        setExpiry(calcExpiry(System.currentTimeMillis()));
        _attributes = attributes;
    }

    @Override
    public String toString() {
        return "spaceId=" + spaceId +
                ", id=" + _id +
                ", contextpath=" + _contextPath +
                ", vhost=" + _vhost +
                ", accessed=" + _accessed +
                ", lastaccessed=" + _lastAccessed +
                ", created=" + _created +
                ", cookieset=" + _cookieSet +
                ", lastnode=" + _lastNode +
                ", expiry=" + _expiry +
                ", maxinactive=" + _maxInactiveMs;
    }

    public long calcExpiry (long time) {
        return (getMaxInactiveMs() <= 0 ? 0 : (time + getMaxInactiveMs()));
    }

    public boolean isExpiredAt(long time) {
        if (getMaxInactiveMs() <= 0)
            return false; //never expires
        return (getExpiry() <= time);
    }


    @SpaceId
    public String getSpaceId() {
        return spaceId;
    }
    public void setSpaceId(String spaceId) {
        this.spaceId = spaceId;
    }

    public SessionData toSessionData() {
        return new SessionData(_id, _contextPath, _vhost, _created, _accessed, _lastAccessed, _maxInactiveMs, _attributes);
    }

    public Set<String> getKeys() {
        return _attributes.keySet();
    }

    public Map<String,Object> getAttributes() {
        return _attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this._attributes = attributes;
    }

    public Object getAttribute(String name) {
        return _attributes.get(name);
    }

    public Object setAttribute (String name, Object value) {
        Object old = (value==null?_attributes.remove(name):_attributes.put(name,value));
        if (value == null && old == null)
            return null; //if same as remove attribute but attribute was already removed, no change

        return old;
    }

    @SpaceIndex
    public String getId() {
        return _id;
    }
    public void setId(String id) {
        _id = id;
    }

    public String getContextPath() {
        return _contextPath;
    }

    public void setContextPath(String contextPath) {
        _contextPath = contextPath;
    }

    public String getVhost() {
        return _vhost;
    }
    public void setVhost(String vhost) {
        _vhost = vhost;
    }

    public String getLastNode() {
        return _lastNode;
    }
    public void setLastNode(String lastNode) {
        _lastNode = lastNode;
    }

    public long getExpiry() {
        return _expiry;
    }
    public void setExpiry(long expiry) {
        _expiry = expiry;
    }

    public long getCreated() {
        return _created;
    }
    public void setCreated(long created) {
        _created = created;
    }

    public long getCookieSet() {
        return _cookieSet;
    }
    public void setCookieSet(long cookieSet) {
        _cookieSet = cookieSet;
    }

    public long getAccessed() {
        return _accessed;
    }
    public void setAccessed(long accessed) {
        _accessed = accessed;
    }

    public long getLastAccessed() {
        return _lastAccessed;
    }
    public void setLastAccessed(long lastAccessed) {
        _lastAccessed = lastAccessed;
    }

    public long getMaxInactiveMs() {
        return _maxInactiveMs;
    }
    public void setMaxInactiveMs(long maxInactive) {
        _maxInactiveMs = maxInactive;
    }
}
