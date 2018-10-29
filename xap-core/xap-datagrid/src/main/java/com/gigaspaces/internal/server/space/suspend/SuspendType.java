package com.gigaspaces.internal.server.space.suspend;

/**
 * This enum provides the current space suspend type.
 * <ul>
 *     <li>NONE - the space is not suspended.</li>
 *     <li>QUIESCED - the space is quiesced.</li>
 *     <li>DISCONNECTED - the space is disconnected from ZooKeeper.</li>
 *     <li>DEMOTING - the space is demoting to backup</li>
 * </ul>
 *
 * @author yohanakh
 * @since 14.0.0
 **/
public enum SuspendType {
    NONE, QUIESCED, DEMOTING, DISCONNECTED
}