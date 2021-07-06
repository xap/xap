package com.gigaspaces.internal.cluster;

import java.util.Map;

public class PuMetaData {
    private String puName;
    private String deployedAt;
    private String unDeployedAt;
    private boolean isPersistent;
    Map<Integer, String> lastPrimaries; //todo- partition number
    //todo- list of space instances and their ips
    private String schema;
    private int numOfInstances;
    private int numOfBackups;


    public PuMetaData() {
    }

    public PuMetaData(String puName, String deployedAt, String unDeployedAt, boolean isPersistent, Map<Integer, String> lastPrimaries, String schema, int numOfInstances, int numOfBackups) {
        this.puName = puName;
        this.deployedAt = deployedAt;
        this.unDeployedAt = unDeployedAt;
        this.isPersistent = isPersistent;
        this.lastPrimaries = lastPrimaries;
        this.schema = schema;
        this.numOfInstances = numOfInstances;
        this.numOfBackups = numOfBackups;
    }

    public String getPuName() {
        return puName;
    }

    public void setPuName(String puName) {
        this.puName = puName;
    }

    public String getDeployedAt() {
        return deployedAt;
    }

    public void setDeployedAt(String deployedAt) {
        this.deployedAt = deployedAt;
    }

    public String getUnDeployedAt() {
        return unDeployedAt;
    }

    public void setUnDeployedAt(String unDeployedAt) {
        this.unDeployedAt = unDeployedAt;
    }

    public boolean isPersistent() {
        return isPersistent;
    }

    public void setIsPersistent(boolean isPersistent) {
        this.isPersistent = isPersistent;
    }

    public Map<Integer, String> getLastPrimaries() {
        return lastPrimaries;
    }

    public void setLastPrimaries(Map<Integer, String> lastPrimaries) {
        this.lastPrimaries = lastPrimaries;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public int getNumOfInstances() {
        return numOfInstances;
    }

    public void setNumOfInstances(int numOfInstances) {
        this.numOfInstances = numOfInstances;
    }

    public int getNumOfBackups() {
        return numOfBackups;
    }

    public void setNumOfBackups(int numOfBackups) {
        this.numOfBackups = numOfBackups;
    }
}