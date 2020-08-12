package com.gigaspaces.attribute_store;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.*;

public class SimulatedSharedLockProviderTest {

    @Test
    public void testLock() throws InterruptedException, TimeoutException, IOException {
        SharedLockProvider lockProvider = SimulatedSharedLockProvider.getInstance();
        System.out.println("Acquiring lock1");
        try (SharedLock lock1 = lockProvider.acquire("lock1", 1, TimeUnit.MILLISECONDS)) {
            System.out.println("Acquired lock1");

            System.out.println("Acquiring lock2");
            try (SharedLock lock2 = lockProvider.acquire("lock2", 1, TimeUnit.MILLISECONDS)) {
                System.out.println("Acquired lock2");
            }
            System.out.println("Released lock2");

            System.out.println("Acquiring lock1 while locked");
            try {
                lockProvider.acquire("lock1", 1, TimeUnit.MILLISECONDS);
                Assert.fail("Should have failed - already locked");
            } catch (TimeoutException e) {
                System.out.println("Already locked - Intercepted expected exception: " + e);
            }
        }
        System.out.println("Released lock1");
    }

    @Test
    public void testLockWait() throws Exception {
        SharedLockProvider lockProvider = SimulatedSharedLockProvider.getInstance();
        CountDownLatch worker1Started = new CountDownLatch(1);
        String lockKey = "lock";
        Callable<Long> worker = () -> {
            System.out.println("Worker about to acquire lock");
            long beforeLock = System.currentTimeMillis();
            worker1Started.countDown();
            try (SharedLock lock = lockProvider.acquire(lockKey, 5, TimeUnit.SECONDS)) {
                long elapsed = System.currentTimeMillis() - beforeLock;
                System.out.println("Worker has acquired lock - duration=" + elapsed + "ms");
                return elapsed;
            }
        };

        Future<Long> future;
        long lock1Time = 100;
        try (SharedLock lock = lockProvider.acquire(lockKey, 1, TimeUnit.MILLISECONDS)) {
            future = Executors.newSingleThreadExecutor().submit(worker);
            worker1Started.await();
            Thread.sleep(lock1Time);
        }
        long duration = future.get();
        Assert.assertTrue("Worker completed too fast - " + duration,duration >= lock1Time);
        Assert.assertTrue("Worker completed too slow - " + duration,duration< lock1Time * 2);
    }
}