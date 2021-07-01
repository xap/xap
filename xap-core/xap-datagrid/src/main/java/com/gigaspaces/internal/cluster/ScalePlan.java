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

import java.util.*;

import static com.gigaspaces.internal.cluster.ClusterTopology.CHUNKS_COUNT;

public class ScalePlan {

    private final ClusterTopology currentMap;
    private final ClusterTopology newMap;
    private final Map<Integer, Map<Integer, Set<Integer>>> plans;

    public static Map<Integer, Set<Integer>> createPartitionChunksMap(int numOfPartitions) {
        Map<Integer, Set<Integer>> partitionsToChunksMap = new HashMap<>(numOfPartitions);
        for (int i = 1; i <= numOfPartitions; i++) {
            partitionsToChunksMap.put(i, new TreeSet<>());
        }
        return partitionsToChunksMap;
    }

    public static Map<Integer, Set<Integer>> createAndInitPartitionChunksMap(int numOfPartitions) {
        Map<Integer, Set<Integer>> map = createPartitionChunksMap(numOfPartitions);
        initPartitionChunksMap(numOfPartitions, map);
        return map;
    }

    private static void initPartitionChunksMap(int numOfPartitions, Map<Integer, Set<Integer>> map) {
        for (int chunk = 0; chunk < CHUNKS_COUNT; chunk++) {
            int partitionId = (chunk % numOfPartitions) + 1;
            map.get(partitionId).add(chunk);
        }
    }

    public static ScalePlan createScaleOutPlan(ClusterTopology currentTopology, int factor) {
        int currPartitionCount = currentTopology.getNumberOfInstances();
        int newPartitionCount = currPartitionCount + factor;
        Map<Integer, Map<Integer, Set<Integer>>> plans = initPlans(factor, 1, currPartitionCount);
        Map<Integer, Set<Integer>> newMap = createPartitionChunksMap(newPartitionCount);
        Map<Integer, Integer> newChunksCountPerPartition = getChunksCountPerPartitionMap(newPartitionCount);
        int newPartitionId = currPartitionCount + 1;
        for (Map.Entry<Integer, Set<Integer>> partitionMapping : currentTopology.getPartitionsToChunksMap().entrySet()) {
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
        ClusterTopology newTopology = currentTopology.copy().setGeneration(currentTopology.getGeneration()+1)
                .setNumOfInstances(newPartitionCount).setPartitionsToChunksMap(newMap);
        return new ScalePlan(currentTopology, newTopology, plans);
    }

    public static ScalePlan createScaleInPlan(ClusterTopology currentTopology, int factor) {
        int currPartitionCount = currentTopology.getNumberOfInstances();
        int newPartitionCount = currPartitionCount - factor;
        Map<Integer, Map<Integer, Set<Integer>>> plans = initPlans(factor, newPartitionCount + 1, currPartitionCount);
        Map<Integer, Set<Integer>> newMap = createPartitionChunksMap(newPartitionCount);
        Map<Integer, Integer> newChunksCountPerPartition = getChunksCountPerPartitionMap(newPartitionCount);
        int remainingPartitionId = 1;
        for (Map.Entry<Integer, Set<Integer>> partitionMapping : currentTopology.getPartitionsToChunksMap().entrySet()) {
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

        ClusterTopology newTopology = currentTopology.copy().setGeneration(currentTopology.getGeneration()+1)
                .setNumOfInstances(newPartitionCount).setPartitionsToChunksMap(newMap);
        return new ScalePlan(currentTopology, newTopology, plans);
    }

    private ScalePlan(ClusterTopology currentMap, ClusterTopology newMap, Map<Integer, Map<Integer, Set<Integer>>> plans) {
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

    public ClusterTopology getCurrentMap() {
        return currentMap;
    }

    public ClusterTopology getNewMap() {
        return newMap;
    }

    public Map<Integer, Map<Integer, Set<Integer>>> getPlans() {
        return plans;
    }

    private static Map<Integer, Integer> getChunksCountPerPartitionMap(int numOfPartitions) {
        Map<Integer, Integer> newChunksCountPerPartition = new HashMap<>(numOfPartitions);
        int newChunksPerPartition = CHUNKS_COUNT / numOfPartitions;
        int newLeftover = CHUNKS_COUNT % numOfPartitions;
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
