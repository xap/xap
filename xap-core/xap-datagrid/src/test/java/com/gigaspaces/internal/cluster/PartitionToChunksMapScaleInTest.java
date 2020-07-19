package com.gigaspaces.internal.cluster;

import org.junit.Assert;
import org.junit.Test;

public class PartitionToChunksMapScaleInTest {

    @Test
    public void testScaleTwoToOne() {
        int numberOfPartitions = 2;
        PartitionToChunksMap map = new PartitionToChunksMap(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleInPlan(map, 1);
        printPlan(plan);
        Assert.assertEquals(2048, plan.getPlans().get(2).get(1).size());
    }

    @Test
    public void testScaleThreeToOne() {
        int numberOfPartitions = 3;
        PartitionToChunksMap map = new PartitionToChunksMap(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleInPlan(map, 2);
        printPlan(plan);
        Assert.assertEquals(1365, plan.getPlans().get(2).get(1).size());
        Assert.assertEquals(1365, plan.getPlans().get(3).get(1).size());
    }

    @Test
    public void testScaleFourToTwo() {
        int numberOfPartitions = 4;
        PartitionToChunksMap map = new PartitionToChunksMap(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleInPlan(map, 2);
        printPlan(plan);
        Assert.assertEquals(1024, plan.getPlans().get(3).get(1).size());
        Assert.assertEquals(1024, plan.getPlans().get(4).get(2).size());
    }

    @Test
    public void testScaleSixToTwo() {
        int numberOfPartitions = 6;
        PartitionToChunksMap map = new PartitionToChunksMap(numberOfPartitions);
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
        PartitionToChunksMap map = new PartitionToChunksMap(numberOfPartitions);
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
        System.out.println("--------------- scale "+plan.getCurrentMap().getNumOfPartitions()+" to "+plan.getNewMap().getNumOfPartitions()+" ---------------");
        System.out.println("old map = "+toShortString(plan.getCurrentMap()));
        System.out.println("new map = "+toShortString(plan.getNewMap()));
        System.out.println(plan);
        System.out.println("--------------------------------------------");
    }
    private String toShortString(PartitionToChunksMap map) {
        StringBuilder stringBuilder = new StringBuilder("Cluster Map\n");
        int numOfPartitions = map.getNumOfPartitions();
        for (int partition=1 ; partition <= numOfPartitions ; partition++) {
            stringBuilder.append("[").append(partition).append("] ---> ");
            stringBuilder.append(map.getPartitionChunks(partition).size());
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }
}
