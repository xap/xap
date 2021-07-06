package com.gigaspaces.internal.cluster;

public class LastPrimary {
    String uid;
    String host;

    public LastPrimary(String uid) {
        this.uid = uid;
    }

    public LastPrimary(String uid, String host) {
        this.uid = uid;
        this.host = host;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
