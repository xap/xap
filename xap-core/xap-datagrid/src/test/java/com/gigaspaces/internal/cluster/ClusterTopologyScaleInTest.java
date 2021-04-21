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

import org.junit.Assert;
import org.junit.Test;

public class ClusterTopologyScaleInTest {

    @Test
    public void testScaleTwoToOne() {
        int numberOfPartitions = 2;
        ClusterTopology map = new ClusterTopology(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleInPlan(map, 1);
        printPlan(plan);
        Assert.assertEquals(2048, plan.getPlans().get(2).get(1).size());
    }

    @Test
    public void testScaleThreeToOne() {
        int numberOfPartitions = 3;
        ClusterTopology map = new ClusterTopology(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleInPlan(map, 2);
        printPlan(plan);
        Assert.assertEquals(1365, plan.getPlans().get(2).get(1).size());
        Assert.assertEquals(1365, plan.getPlans().get(3).get(1).size());
    }

    @Test
    public void testScaleFourToTwo() {
        int numberOfPartitions = 4;
        ClusterTopology map = new ClusterTopology(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleInPlan(map, 2);
        printPlan(plan);
        Assert.assertEquals(1024, plan.getPlans().get(3).get(1).size());
        Assert.assertEquals(1024, plan.getPlans().get(4).get(2).size());
    }

    @Test
    public void testScaleSixToTwo() {
        int numberOfPartitions = 6;
        ClusterTopology map = new ClusterTopology(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleInPlan(map, 4);
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
        ClusterTopology map = new ClusterTopology(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleInPlan(map, 4);
        printPlan(plan);
        Assert.assertEquals(585, plan.getPlans().get(4).get(1).size());
        Assert.assertEquals(195, plan.getPlans().get(5).get(1).size());
        Assert.assertEquals(390, plan.getPlans().get(5).get(2).size());
        Assert.assertEquals(390, plan.getPlans().get(6).get(2).size());
        Assert.assertEquals(195, plan.getPlans().get(6).get(3).size());
        Assert.assertEquals(585, plan.getPlans().get(7).get(3).size());
    }



    private void printPlan(ScalePlan plan) {
        System.out.println("--------------- scale "+plan.getCurrentMap().getNumberOfInstances()+" to "+plan.getNewMap().getNumberOfInstances()+" ---------------");
        System.out.println("old map = "+toShortString(plan.getCurrentMap()));
        System.out.println("new map = "+toShortString(plan.getNewMap()));
        System.out.println(plan);
        System.out.println("--------------------------------------------");
    }
    private String toShortString(ClusterTopology map) {
        StringBuilder stringBuilder = new StringBuilder("Cluster Map\n");
        int numOfPartitions = map.getNumberOfInstances();
        for (int partition=1 ; partition <= numOfPartitions ; partition++) {
            stringBuilder.append("[").append(partition).append("] ---> ");
            stringBuilder.append(map.getPartitionChunks(partition).size());
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }
}
