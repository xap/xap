package com.gigaspaces.attribute_store;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface SharedLock {
    void acquire(long time, TimeUnit unit) throws IOException, TimeoutException;
    void release() throws IOException;
}
