package com.gigaspaces.attribute_store;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * A simulation of a shared lock, which is not really shared...
 * Used to simulate a shared lock when zookeeper is not available.
 * @author Niv Ingberg
 * @since 15.5
 */
public class SimulatedSharedLockProvider implements SharedLockProvider {
    private static final SimulatedSharedLockProvider instance = new SimulatedSharedLockProvider();

    private final ConcurrentMap<String, SharedLockImpl> locksMap = new ConcurrentHashMap<>();

    public static SharedLockProvider getInstance() {
        return instance;
    }

    @Override
    public SharedLock acquire(String key, long timeout, TimeUnit timeunit) throws TimeoutException, InterruptedException {
        return acquire(new SharedLockImpl(key), timeout, timeunit);
    }

    private SharedLock acquire(SharedLockImpl newLock, long timeout, TimeUnit timeunit) throws TimeoutException, InterruptedException {
        SharedLockImpl currLock = locksMap.putIfAbsent(newLock.key, newLock);
        if (currLock == null)
            return newLock;
        long beforeWait = System.currentTimeMillis();
        if (!currLock.latch.await(timeout, timeunit))
            throw new TimeoutException("Timed out while waiting to acquire [" + newLock.key + "]");
        long elapsed = System.currentTimeMillis() - beforeWait;
        return acquire(newLock, timeunit.toMillis(timeout) - elapsed, TimeUnit.MILLISECONDS);
    }

    private class SharedLockImpl implements SharedLock {
        private final String key;
        private final CountDownLatch latch = new CountDownLatch(1);

        private SharedLockImpl(String key) {
            this.key = key;
        }

        @Override
        public void close() throws IOException {
            locksMap.remove(key);
            latch.countDown();
        }
    }
}
