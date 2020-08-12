package com.j_spaces.core.cache;

/**
 * @author Alon on 5/11/2017.
 * @since 12.1
 */
public enum CacheOperationReason {
    ON_WRITE,
    ON_UPDATE,
    ON_INITIAL_LOAD,
    ON_TAKE,
    ON_READ
}