package com.gigaspaces.internal.cluster.node.impl.backlog;

/**
 * @author yael nahon
 * @since 12.1
 */
public class BacklogWeightPolicyFactory {
    public static BacklogWeightPolicy create(String policy) {
        if (policy.equalsIgnoreCase("fixed")) {
            return new FixedBacklogWeightPolicy();
        } else if (policy.equalsIgnoreCase("accumulated")) {
            return new AccumulatedBacklogWeightPolicy();
        } else {
            throw new UnsupportedOperationException("No such backlog size policy named: " + policy);
        }
    }
}
