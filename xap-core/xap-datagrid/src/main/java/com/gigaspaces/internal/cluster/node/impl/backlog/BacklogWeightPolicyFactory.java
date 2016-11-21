package com.gigaspaces.internal.cluster.node.impl.backlog;

/**
 * @author yael nahon
 * @since 12.1
 */
public class BacklogWeightPolicyFactory {
    public static BacklogWeightPolicy create(String policy) {
        if (policy.equalsIgnoreCase("weight-by-packets")) {
            return new WeightByPacketsBacklogWeightPolicy();
        } else if (policy.equalsIgnoreCase("weight-by-operations")) {
            return new WeightByOperationsBacklogWeightPolicy();
        } else {
            throw new UnsupportedOperationException("No such backlog size policy named: " + policy);
        }
    }
}
