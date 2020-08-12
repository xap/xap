package com.gigaspaces.server.space.suspend;

/**
 * This enum provides the current space suspend type.
 *
 * @author yohanakh
 * @since 14.0.0
 **/
public enum SuspendType {

    /**
     * The space is not suspended.
     */
    NONE,

    /**
     * The space is quiesced.
     */
    QUIESCED,

    /**
     * The space is demoting to backup.
     */
    DEMOTING,

    /**
     * The space is disconnected from ZooKeeper.
     */
    DISCONNECTED
}