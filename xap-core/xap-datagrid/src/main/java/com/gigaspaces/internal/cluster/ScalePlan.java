package com.gigaspaces.internal.cluster;

import java.util.*;

public class ScalePlan {

    private final PartitionToChunksMap currentMap;
    private final PartitionToChunksMap newMap;
    private final Map<Integer, Map<Integer, Set<Integer>>> plans;

    public static Map<Integer, Set<Integer>> createPartitionChunksMap(int numOfPartitions) {
        Map<Integer, Set<Integer>> partitionsToChunksMap = new HashMap<>(numOfPartitions);
        for (int i = 1; i <= numOfPartitions; i++) {
            partitionsToChunksMap.put(i, new TreeSet<>());
        }
        return partitionsToChunksMap;
    }

    public static ScalePlan createScaleOutPlan(PartitionToChunksMap currentMap, int factor) {
        int currPartitionCount = currentMap.getNumOfPartitions();
        int newPartitionCount = currPartitionCount + factor;
        Map<Integer, Map<Integer, Set<Integer>>> plans = initPlans(factor, 1, currPartitionCount);
        Map<Integer, Set<Integer>> newMap = createPartitionChunksMap(newPartitionCount);
        Map<Integer, Integer> newChunksCountPerPartition = getChunksCountPerPartitionMap(newPartitionCount);
        int newPartitionId = currPartitionCount + 1;
        for (Map.Entry<Integer, Set<Integer>> partitionMapping : currentMap.getPartitionsToChunksMap().entrySet()) {
            int currentPartitionId = partitionMapping.getKey();
            int oldPartitionAssignments = 0;
            for (Integer chunk : partitionMapping.getValue()) {
                if (oldPartitionAssignments < newChunksCountPerPartition.get(currentPartitionId)) {
                    newMap.get(currentPartitionId).add(chunk);
                    oldPartitionAssignments++;
                } else {
                    if (newMap.get(newPartitionId).size() == newChunksCountPerPartition.get(newPartitionId) && newPartitionId < newPartitionCount) {
                        newPartitionId++;
                    }
                    newMap.get(newPartitionId).add(chunk);
                    Map<Integer, Set<Integer>> partitionPlan = plans.get(currentPartitionId);
                    if (!partitionPlan.containsKey(newPartitionId)) {
                        partitionPlan.put(newPartitionId, new HashSet<>());
                    }
                    partitionPlan.get(newPartitionId).add(chunk);
                }
            }
        }
        return new ScalePlan(currentMap, new PartitionToChunksMap(newMap, currentMap.getGeneration() + 1), plans);
    }

    public static ScalePlan createScaleInPlan(PartitionToChunksMap currentMap, int factor) {
        int currPartitionCount = currentMap.getNumOfPartitions();
        int newPartitionCount = currPartitionCount - factor;
        Map<Integer, Map<Integer, Set<Integer>>> plans = initPlans(factor, newPartitionCount + 1, currPartitionCount);
        Map<Integer, Set<Integer>> newMap = createPartitionChunksMap(newPartitionCount);
        Map<Integer, Integer> newChunksCountPerPartition = getChunksCountPerPartitionMap(newPartitionCount);
        int remainingPartitionId = 1;
        for (Map.Entry<Integer, Set<Integer>> partitionMapping : currentMap.getPartitionsToChunksMap().entrySet()) {
            int currentPartitionId = partitionMapping.getKey();
            if (currentPartitionId <= newPartitionCount) {
                newMap.get(currentPartitionId).addAll(partitionMapping.getValue());
            } else {
                for (Integer chunk : partitionMapping.getValue()) {
                    if (newMap.get(remainingPartitionId).size() == newChunksCountPerPartition.get(remainingPartitionId) && remainingPartitionId < newPartitionCount) {
                        remainingPartitionId++;
                    }
                    newMap.get(remainingPartitionId).add(chunk);
                    Map<Integer, Set<Integer>> partitionPlan = plans.get(currentPartitionId);
                    if (!partitionPlan.containsKey(remainingPartitionId)) {
                        partitionPlan.put(remainingPartitionId, new HashSet<>());
                    }
                    partitionPlan.get(remainingPartitionId).add(chunk);
                }
            }
        }

        return new ScalePlan(currentMap, new PartitionToChunksMap(newMap, currentMap.getGeneration() + 1), plans);
    }

    private ScalePlan(PartitionToChunksMap currentMap, PartitionToChunksMap newMap, Map<Integer, Map<Integer, Set<Integer>>> plans) {
        this.currentMap = currentMap;
        this.newMap = newMap;
        this.plans = plans;
    }

    private static Map<Integer, Map<Integer, Set<Integer>>> initPlans(int factor, int from, int to) {
        Map<Integer, Map<Integer, Set<Integer>>> result = new HashMap<>();
        for (int i = from; i <= to; i++) {
            result.put(i, new HashMap<>(factor));
        }
        return result;
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

    private static Map<Integer, Integer> getChunksCountPerPartitionMap(int numOfPartitions) {
        Map<Integer, Integer> newChunksCountPerPartition = new HashMap<>(numOfPartitions);
        int newChunksPerPartition = PartitionToChunksMap.CHUNKS_COUNT / numOfPartitions;
        int newLeftover = PartitionToChunksMap.CHUNKS_COUNT % numOfPartitions;
        for (int i = 1; i <= numOfPartitions; i++) {
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
