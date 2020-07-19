package com.gigaspaces.internal.cluster;

import java.util.*;

public class ScalePlan {

    private final PartitionToChunksMap currentMap;
    private PartitionToChunksMap newMap;
    private final Map<Integer, Map<Integer, Set<Integer>>> plans;

    public static ScalePlan createScaleOutPlan(PartitionToChunksMap currentMap, int factor) {
        ScalePlan scalePlan = new ScalePlan(currentMap).initScaleOutPlan(factor);
        PartitionToChunksMap newMap = new PartitionToChunksMap(currentMap.getNumOfPartitions() + factor, currentMap.getGeneration() + 1);
        Map<Integer, Integer> newChunksCountPerPartition = getChunksCountPerPartitionMap(newMap);
        int newPartitionId = currentMap.getNumOfPartitions() + 1;
        for (Map.Entry<Integer, Set<Integer>> partitionMapping : currentMap.getPartitionsToChunksMap().entrySet()) {
            int currentPartitionId = partitionMapping.getKey();
            int oldPartitionAssignments = 0;
            for (Integer chunk : partitionMapping.getValue()) {
                if (oldPartitionAssignments < newChunksCountPerPartition.get(currentPartitionId)) {
                    newMap.getPartitionsToChunksMap().get(currentPartitionId).add(chunk);
                    oldPartitionAssignments++;
                } else {
                    if (newMap.getPartitionsToChunksMap().get(newPartitionId).size() == newChunksCountPerPartition.get(newPartitionId) && newPartitionId < newMap.getNumOfPartitions()) {
                        newPartitionId++;
                    }
                    newMap.getPartitionsToChunksMap().get(newPartitionId).add(chunk);
                    Map<Integer, Set<Integer>> partitionPlan = scalePlan.plans.get(currentPartitionId);
                    if (!partitionPlan.containsKey(newPartitionId)) {
                        partitionPlan.put(newPartitionId, new HashSet<>());
                    }
                    partitionPlan.get(newPartitionId).add(chunk);
                }
            }
        }
        scalePlan.newMap = newMap;
        return scalePlan;
    }

    public static ScalePlan createScaleInPlan(PartitionToChunksMap currentMap, int factor) {
        ScalePlan scalePlan = new ScalePlan(currentMap).initScaleInPlan(factor);
        int newPartitionCount = currentMap.getNumOfPartitions() - factor;
        PartitionToChunksMap newMap = new PartitionToChunksMap(newPartitionCount, currentMap.getGeneration() + 1);
        Map<Integer, Integer> newChunksCountPerPartition = getChunksCountPerPartitionMap(newMap);
        int remainingPartitionId = 1;
        for (Map.Entry<Integer, Set<Integer>> partitionMapping : currentMap.getPartitionsToChunksMap().entrySet()) {
            int currentPartitionId = partitionMapping.getKey();
            if (currentPartitionId <= newMap.getNumOfPartitions()) {
                newMap.getPartitionsToChunksMap().get(currentPartitionId).addAll(partitionMapping.getValue());
            } else {
                for (Integer chunk : partitionMapping.getValue()) {
                    if (newMap.getPartitionsToChunksMap().get(remainingPartitionId).size() == newChunksCountPerPartition.get(remainingPartitionId) && remainingPartitionId < newMap.getNumOfPartitions()) {
                        remainingPartitionId++;
                    }
                    newMap.getPartitionsToChunksMap().get(remainingPartitionId).add(chunk);
                    Map<Integer, Set<Integer>> partitionPlan = scalePlan.plans.get(currentPartitionId);
                    if (!partitionPlan.containsKey(remainingPartitionId)) {
                        partitionPlan.put(remainingPartitionId, new HashSet<>());
                    }
                    partitionPlan.get(remainingPartitionId).add(chunk);
                }
            }
        }

        scalePlan.newMap = newMap;
        return scalePlan;
    }

    private ScalePlan(PartitionToChunksMap currentMap) {
        this.currentMap = currentMap;
        this.plans = new HashMap<>(currentMap.getNumOfPartitions());
    }

    private ScalePlan initScaleOutPlan(int factor) {
        for (int i = 1; i <= currentMap.getNumOfPartitions(); i++) {
            this.plans.put(i, new HashMap<>(factor));
        }
        return this;
    }

    private ScalePlan initScaleInPlan(int factor) {
        for (int i = currentMap.getNumOfPartitions(); i > currentMap.getNumOfPartitions() - factor; i--) {
            this.plans.put(i, new HashMap<>(factor));
        }
        return this;
    }

    public PartitionToChunksMap getCurrentMap() {
        return currentMap;
    }

    public PartitionToChunksMap getNewMap() {
        return newMap;
    }

    public Map<Integer, Map<Integer, Set<Integer>>> getPlans() {
        return plans;
    }

    private static Map<Integer, Integer> getChunksCountPerPartitionMap(PartitionToChunksMap newMap) {
        Map<Integer, Integer> newChunksCountPerPartition = new HashMap<>(newMap.getNumOfPartitions());
        int newChunksPerPartition = PartitionToChunksMap.CHUNKS_COUNT / newMap.getNumOfPartitions();
        int newLeftover = PartitionToChunksMap.CHUNKS_COUNT % newMap.getNumOfPartitions();
        for (int i = 1; i <= newMap.getNumOfPartitions(); i++) {
            newChunksCountPerPartition.put(i, newChunksPerPartition);
        }
        for (int i = 0, index = 1; i < newLeftover; i++, index++) {
            newChunksCountPerPartition.put(index, newChunksCountPerPartition.get(index) + 1);
        }
        return newChunksCountPerPartition;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ScalePlan: \n");
        for (Map.Entry<Integer, Map<Integer, Set<Integer>>> partitionPlan : plans.entrySet()) {
            sb.append("Partition ").append(partitionPlan.getKey()).append(":");
            for (Map.Entry<Integer, Set<Integer>> copyPlan : partitionPlan.getValue().entrySet()) {
                sb.append("\t\n").append("move ").append(copyPlan.getValue().size()).append(" chunks to partition ").append(copyPlan.getKey());
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
