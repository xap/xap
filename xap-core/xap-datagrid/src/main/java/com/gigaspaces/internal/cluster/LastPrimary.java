package com.gigaspaces.internal.cluster;

public class LastPrimary {
    String instanceId;
    String host;

    public LastPrimary(String instanceId) {
        this.instanceId = instanceId;
    }

    public LastPrimary(String instanceId, String host) {
        this.instanceId = instanceId;
        this.host = host;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
