/*
 * Copyright 2005 Sun Microsystems, Inc.
 * Copyright 2005 GigaSpaces, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jini.rio.resources.resource;



/**
 * A resource pool to handle a pool of threads
 */
@com.gigaspaces.api.InternalApi
public class ThreadPool extends ResourcePool {
    protected ThreadGroup group;
    static int nextID = 0;

    /**
     * Create new named ThreadPool with a specified amount of minimum and maximum elements in the
     * pool
     *
     * @param identifier the unique identifier for this instance
     * @param min        the minimum number of resources to create at startup
     * @param max        the maximum number of resources allowed
     */
    public ThreadPool(String identifier, int min, int max) {
        setIdentifier(identifier);
        setMin(min);
        setMax(max);
        group = new ThreadGroup(identifier);
        group.setDaemon(true);
        try {
            createResources();
        } catch (ResourceUnavailableException e) {
            logger.error(
                    "Creating ThreadPool with min=" + min + ", max=" + max,
                    e);
        }
    }

    protected static synchronized int nextId() {
        return nextID++;
    }

    /**
     * method for creating a specific resource in the sub-class
     *
     * @return the resource
     * @throws ResourceUnavailableException if an error occured in creating the resource
     */
    protected Object create() throws ResourceUnavailableException {
        PoolableThread thread = new PoolableThread(this);
        thread.setDaemon(true);
        thread.start();
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            logger.warn("Creating a resource interrupted", e);
        }
        return (thread);
    }

    /**
     * Dispose of a resource
     */
    protected void dispose(Object obj) {
        if (obj instanceof PoolableThread) {
            PoolableThread thread = (PoolableThread) obj;
            thread.cleanStop();
        }
    }

    /**
     * Validate a resource
     */
    protected boolean validate(Object obj) {
        return ((PoolableThread) obj).isAlive();
    }

    /**
     * get the thread group for this pool
     *
     * @return the thread group for this pool
     */
    public ThreadGroup getThreadGroup() {
        return group;
    }

    /**
     * destroy all the threads spawned by this pool
     */
    public void destroy() {
        if (group.isDestroyed())
            return;
        int activeCount = group.activeCount();
        Thread[] activeThreads = new Thread[activeCount];
        int numThreads = group.enumerate(activeThreads);
        for (int i = 0; i < numThreads; i++) {
            try {
                if (activeThreads[i] instanceof PoolableThread)
                    ((PoolableThread) activeThreads[i]).cleanStop();
                activeThreads[i].interrupt();
            } catch (Exception ex) {
                logger.warn("Failed to stop active thread " + activeThreads[i].getId(), ex);
                ex.printStackTrace();
            }
            remove(activeThreads[i]);
        }
        // interupt any threads that did not quit nicely from above.
        group.interrupt();
        destroyChildGroup(group);
        for (int i = 0; group.activeCount() > 0 && i < 10; i++) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                logger.warn("Interrupted while destroying latent Threads", e);
            }
        }
        try {
            if (!group.isDestroyed())
                group.destroy();
        } catch (IllegalThreadStateException e) {
            if (!group.isDestroyed()) {
                //not a race condition issue, weird
                logger.warn("IllegalThreadState destroying ThreadGroup", e);
            }
        }
    }

    /**
     * interrupt any threads from child groups for this pool
     */
    private void destroyChildGroup(ThreadGroup group) {
        int numChildren = group.activeGroupCount();
        if (numChildren == 0)
            return;
        ThreadGroup[] children = new ThreadGroup[numChildren];
        int numGroups = group.enumerate(children);
        for (int i = 0; i < numGroups; i++) {
            children[i].interrupt();
            destroyChildGroup(children[i]);
        }
    }

    /**
     * Unit Test
     */
    public static void main(String[] args) {
        try {
            ThreadPool tp = new ThreadPool("Unit Test Thread Pool", 5, 10);
            ThreadGroup group = tp.getThreadGroup();
            group.list();
            tp.destroy();
            group.list();
            tp.get();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        System.exit(0);
    }
}
