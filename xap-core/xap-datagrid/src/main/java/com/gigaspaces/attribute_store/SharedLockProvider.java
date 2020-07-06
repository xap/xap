package com.gigaspaces.attribute_store;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Niv Ingberg
 * @since 15.5
 */
public interface SharedLockProvider {
    SharedLock acquire(String key, long timeout, TimeUnit timeunit) throws IOException, TimeoutException, InterruptedException;
}
