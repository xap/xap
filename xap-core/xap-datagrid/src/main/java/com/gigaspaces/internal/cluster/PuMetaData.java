package com.gigaspaces.internal.cluster;

import java.util.Map;

public class PuMetaData {
    private String puName;
    private String deployedAt;
    private String unDeployedAt;
    private boolean isPersistent;
    private Map<Integer, LastPrimary> lastPrimaryPerPartition;
    private Map<String, String> spaceInstancesHosts;
    private String schema;
    private int numOfInstances;
    private int numOfBackups;


    public PuMetaData() {
    }

    public PuMetaData(String puName, String deployedAt, String unDeployedAt, boolean isPersistent, Map<Integer, LastPrimary> lastPrimaryPerPartition,
                      Map<String, String> spaceInstancesHosts, String schema, int numOfInstances, int numOfBackups) {
        this.puName = puName;
        this.deployedAt = deployedAt;
        this.unDeployedAt = unDeployedAt;
        this.isPersistent = isPersistent;
        this.lastPrimaryPerPartition = lastPrimaryPerPartition;
        this.spaceInstancesHosts = spaceInstancesHosts;
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

    public Map<Integer, LastPrimary> getLastPrimaryPerPartition() {
        return lastPrimaryPerPartition;
    }

    public void setLastPrimaryPerPartition(Map<Integer, LastPrimary> lastPrimaryPerPartition) {
        this.lastPrimaryPerPartition = lastPrimaryPerPartition;
    }

    public void setPersistent(boolean persistent) {
        isPersistent = persistent;
    }

    public Map<String, String> getSpaceInstancesHosts() {
        return spaceInstancesHosts;
    }

    public void setSpaceInstancesHosts(Map<String, String> spaceInstancesHosts) {
        this.spaceInstancesHosts = spaceInstancesHosts;
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