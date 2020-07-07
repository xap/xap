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
package com.gigaspaces.internal;

import com.gigaspaces.internal.cluster.PartitionToChunksMap;
import com.gigaspaces.internal.cluster.ScalePlan;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class PartitionToChunksMapScaleInTest {

    @Test
    public void testScaleTwoToOne() {
        int numberOfPartitions = 2;
        PartitionToChunksMap map = new PartitionToChunksMap(numberOfPartitions, 0);
        map.init();
        ScalePlan plan = PartitionToChunksMap.scaleInMap(map, 1);
        printPlan(plan);
        Assert.assertEquals(2048, plan.getPlans().get(2).get(1).size());
    }

    @Test
    public void testScaleThreeToOne() {
        int numberOfPartitions = 3;
        PartitionToChunksMap map = new PartitionToChunksMap(numberOfPartitions, 0);
        map.init();
        ScalePlan plan = PartitionToChunksMap.scaleInMap(map, 2);
        printPlan(plan);
        Assert.assertEquals(1365, plan.getPlans().get(2).get(1).size());
        Assert.assertEquals(1365, plan.getPlans().get(3).get(1).size());
    }

    @Test
    public void testScaleFourToTwo() {
        int numberOfPartitions = 4;
        PartitionToChunksMap map = new PartitionToChunksMap(numberOfPartitions, 0);
        map.init();
        ScalePlan plan = PartitionToChunksMap.scaleInMap(map, 2);
        printPlan(plan);
        Assert.assertEquals(1024, plan.getPlans().get(3).get(1).size());
        Assert.assertEquals(1024, plan.getPlans().get(4).get(2).size());
    }

    @Test
    public void testScaleSixToTwo() {
        int numberOfPartitions = 6;
        PartitionToChunksMap map = new PartitionToChunksMap(numberOfPartitions, 0);
        map.init();
        ScalePlan plan = PartitionToChunksMap.scaleInMap(map, 4);
        printPlan(plan);
        Assert.assertEquals(683, plan.getPlans().get(3).get(1).size());
        Assert.assertEquals(682, plan.getPlans().get(4).get(1).size());
        Assert.assertEquals(1, plan.getPlans().get(4).get(2).size());
        Assert.assertEquals(682, plan.getPlans().get(5).get(2).size());
        Assert.assertEquals(682, plan.getPlans().get(6).get(2).size());
    }

    @Test
    public void testScaleSevenToThree() {
        int numberOfPartitions = 7;
        PartitionToChunksMap map = new PartitionToChunksMap(numberOfPartitions, 0);
        map.init();
        ScalePlan plan = PartitionToChunksMap.scaleInMap(map, 4);
        printPlan(plan);
        Assert.assertEquals(585, plan.getPlans().get(4).get(1).size());
        Assert.assertEquals(195, plan.getPlans().get(5).get(1).size());
        Assert.assertEquals(390, plan.getPlans().get(5).get(2).size());
        Assert.assertEquals(390, plan.getPlans().get(6).get(2).size());
        Assert.assertEquals(195, plan.getPlans().get(6).get(3).size());
        Assert.assertEquals(585, plan.getPlans().get(7).get(3).size());
    }



    private void printPlan(ScalePlan plan) {
        System.out.println("--------------- scale "+plan.getCurrentMap().getNumOfPartitions()+" to "+plan.getNewMap().getNumOfPartitions()+" ---------------");
        System.out.println("old map = "+toShortString(plan.getCurrentMap()));
        System.out.println("new map = "+toShortString(plan.getNewMap()));
        System.out.println(plan);
        System.out.println("--------------------------------------------");
    }
    private String toShortString(PartitionToChunksMap map) {
        StringBuilder stringBuilder = new StringBuilder("Cluster Map\n");
        for (Map.Entry<Integer, Set<Integer>> entry : map.getPartitionsToChunksMap().entrySet()) {
            stringBuilder.append("[").append(entry.getKey()).append("] ---> ");
            stringBuilder.append(entry.getValue().size());
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }
}
