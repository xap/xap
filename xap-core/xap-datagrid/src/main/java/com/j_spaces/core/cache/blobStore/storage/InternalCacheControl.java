package com.j_spaces.core.cache.blobStore.storage;

/**
 * @author kobi on 29/12/16.
 * @since 12.1
 */
public enum InternalCacheControl {
    INSERT_IF_NEEDED_BY_OP,
    INSERT_TO_INTERNAL_CACHE_FROM_INITIAL_LOAD,
    DONT_INSERT_TO_INTERNAL_CACHE_FROM_INITIAL_LOAD,
    DONT_INSERT_TO_INTERNAL_CACHE
}