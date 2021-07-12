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
package com.gigaspaces.internal.cluster;

import java.util.Map;

public class UndeployedPuMetaData {
    private String puName;
    private String unDeployedAt;
    private boolean isPersistent;
    private Map<Integer, LastPrimary> lastPrimaryPerPartition;
    private Map<String, String> spaceInstancesHosts;
    private String schema;
    private int numOfInstances;
    private int numOfBackups;


    public UndeployedPuMetaData() {
    }

    public UndeployedPuMetaData(String puName, String unDeployedAt, boolean isPersistent, Map<Integer, LastPrimary> lastPrimaryPerPartition,
                                Map<String, String> spaceInstancesHosts, String schema, int numOfInstances, int numOfBackups) {
        this.puName = puName;
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