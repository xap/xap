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
package com.gigaspaces.proxy_pool;


import java.util.Objects;

public class SessionIdEntry implements Comparable {
    final long time;
    final String sessionId;
    final String spaceName;


    public SessionIdEntry(long time, String sessionId, String spaceName) {
        this.time = time;
        this.sessionId = sessionId;
        this.spaceName = spaceName;
    }

    public SessionIdEntry(String sessionId, String spaceName) {
        this.sessionId = sessionId;
        this.spaceName = spaceName;
        this.time = System.currentTimeMillis();
    }

    public String getSpaceName() {
        return spaceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SessionIdEntry that = (SessionIdEntry) o;
        return Objects.equals(sessionId, that.sessionId) && Objects.equals(spaceName, that.spaceName) && time == that.time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, spaceName);
    }


    public long getTime() {
        return time;
    }

    public String getSessionId() {
        return sessionId;
    }

    @Override
    public int compareTo(Object o) {
        SessionIdEntry user = (SessionIdEntry) o;
        int compare = Long.compare(this.time, user.time);
        if (compare != 0)
            return compare;
        compare = this.sessionId.compareTo(user.sessionId);
        if (compare != 0)
            return compare;
        return this.spaceName.compareTo(user.spaceName);
    }
}
