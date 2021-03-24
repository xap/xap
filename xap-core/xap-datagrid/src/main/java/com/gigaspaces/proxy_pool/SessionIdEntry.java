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
