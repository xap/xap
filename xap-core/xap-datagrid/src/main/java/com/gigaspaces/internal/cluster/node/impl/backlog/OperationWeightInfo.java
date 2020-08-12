package com.gigaspaces.internal.cluster.node.impl.backlog;

/**
 * @author yael nahon
 * @since 12.1
 */
public class OperationWeightInfo {

    private int numOfOperations;
    private WeightInfoOperationType type;

    public OperationWeightInfo(int numOfOperations, WeightInfoOperationType type) {
        this.numOfOperations = numOfOperations;
        this.type = type;
    }

    public OperationWeightInfo(WeightInfoOperationType type) {
        this.numOfOperations = -1;
        this.type = type;
    }

    public OperationWeightInfo() {
    }

    public int getNumOfOperations() {
        return numOfOperations;
    }

    public WeightInfoOperationType getType() {
        return type;
    }
}
