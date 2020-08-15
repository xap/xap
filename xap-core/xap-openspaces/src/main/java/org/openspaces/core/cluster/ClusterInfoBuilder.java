package org.openspaces.core.cluster;

import com.gigaspaces.cluster.DynamicPartitionInfo;
import org.openspaces.core.cluster.internal.ClusterInfoImpl;

public class ClusterInfoBuilder {
    private String name;
    private int generation;
    private String schema;
    private Integer instanceId;
    private Integer backupId;
    private Integer numberOfInstances;
    private Integer numberOfBackups;
    private DynamicPartitionInfo dynamicPartitionInfo;

    public ClusterInfoBuilder() {
    }

    public ClusterInfoBuilder(ClusterInfo clusterInfo) {
        this.name = clusterInfo.getName();
        this.generation = clusterInfo.getGeneration();
        this.schema = clusterInfo.getSchema();
        this.instanceId = clusterInfo.getInstanceId();
        this.backupId = clusterInfo.getBackupId();
        this.numberOfInstances = clusterInfo.getNumberOfInstances();
        this.numberOfBackups = clusterInfo.getNumberOfBackups();
        this.dynamicPartitionInfo = clusterInfo.getDynamicPartitionInfo();
    }

    public ClusterInfo build() {
        return generation != 0 || dynamicPartitionInfo != null
                ? new ClusterInfoImpl(this)
                : new ClusterInfo(this);
    }

    public ClusterInfoBuilder name(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public ClusterInfoBuilder generation(int generation) {
        this.generation = generation;
        return this;
    }

    public int getGeneration() {
        return generation;
    }

    public ClusterInfoBuilder schema(String schema) {
        this.schema = schema;
        return this;
    }

    public String getSchema() {
        return schema;
    }

    public ClusterInfoBuilder numberOfInstances(Integer numberOfInstances) {
        this.numberOfInstances = numberOfInstances;
        return this;
    }

    public Integer getNumberOfInstances() {
        return numberOfInstances;
    }

    public ClusterInfoBuilder numberOfBackups(Integer numberOfBackups) {
        this.numberOfBackups = numberOfBackups;
        return this;
    }

    public Integer getNumberOfBackups() {
        return numberOfBackups;
    }

    public ClusterInfoBuilder instanceId(Integer instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    public Integer getInstanceId() {
        return instanceId;
    }

    public ClusterInfoBuilder backupId(Integer backupId) {
        this.backupId = backupId;
        return this;
    }

    public Integer getBackupId() {
        return backupId;
    }

    public ClusterInfoBuilder dynamicPartitionInfo(DynamicPartitionInfo dynamicPartitionInfo) {
        this.dynamicPartitionInfo = dynamicPartitionInfo;
        return this;
    }

    public DynamicPartitionInfo getDynamicPartitionInfo() {
        return dynamicPartitionInfo;
    }
}
