package com.gigaspaces.attribute_store;

import java.util.concurrent.TimeUnit;

public interface SharedLock {
    boolean	acquire(long time, TimeUnit unit) throws Exception;
    void release() throws Exception;
}
