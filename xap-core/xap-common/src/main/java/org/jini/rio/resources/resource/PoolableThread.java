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

import java.util.Hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extension of Thread, PoolableThread instances know how to stay alive and wait in a pool
 */
public final class PoolableThread extends Thread {
    protected static int nextThreadID = 0;
    final protected ThreadPool pool;
    final int threadID;
    private boolean keepAlive = true;
    /**
     * The runnable to run
     */
    protected Runnable runObj;
    /**
     * Resource cache for Threads
     */
    final private Hashtable objectCache = new Hashtable();
    /**
     * A mutex
     */
    private final Object working = new Object();
    /**
     * Used to determine if the thread is complete
     */
    final private Object complete = new Object();
    /**
     * A Logger for the poolable
     */
    private static final Logger logger =
            LoggerFactory.getLogger("org.jini.rio.resources.resource.PoolableThread");

    /**
     * A thread that is a good pool citizen.
     */
    public PoolableThread(ThreadPool pool) {
        super(pool.getThreadGroup(), "");
        this.pool = pool;
        synchronized (PoolableThread.class) {
            threadID = nextThreadID++;
        }
        setName(pool.getIdentifier() + "-" + threadID);
    }

    public boolean isReady() {
        return (runObj == null);
    }

    /**
     * Waits for the resource to complete
     */
    public void joinResource() throws InterruptedException {
        if (runObj != null) {
            synchronized (complete) {
                if (runObj != null)
                    complete.wait();
            }
        }
    }

    /**
     * Wait in the pool, until someone needs this thread, then reenter pool.  Do not override this
     * method, use <code>execute</code> instead.
     */
    public final void run() {
        while (keepAlive) {
            synchronized (working) {
                if (runObj == null) {
                    try {
                        working.wait();          // wait for execute.
                    } catch (InterruptedException ignore) {
                        ;
                    }
                }
                try {
                    if (runObj != null) {
                        runObj.run();               // work!.
                        runObj = null;
                    }
                    synchronized (complete) {
                        complete.notifyAll();      // wake threads who have join1()ed.
                    }
                } catch (Throwable t) {
                    /* catch the throwable, and release */
                    if (logger.isTraceEnabled())
                        logger.trace(
                                "Running poolable thread id=" + threadID,
                                t);
                } finally {
                    try {
                        pool.release(this);     // return resource to pool
                    } catch (Exception e) {
                        if (logger.isTraceEnabled())
                            logger.trace(
                                    "Releasing poolable thread " +
                                            "id=" + threadID,
                                    e);
                    }
                }
            }
        }
    }

    /**
     * Define what to run, and wake up this dozing thread
     */
    public void execute(Runnable runnable) throws IllegalStateException {
        if (runObj != null)
            throw new IllegalStateException("Cannot execute PoolableThread that " +
                    "is busy");
        synchronized (working) {
            /* Check again, in case another thread got here before us.*/
            if (runObj != null)
                throw new IllegalStateException("Cannot execute PoolableThread " +
                        "that is busy");
            runObj = runnable;
            working.notifyAll();                 /* wake from working.wait above. */
        }
    }

    /**
     * Set a flag to stop this thread, after a run.
     */
    public void cleanStop() {
        keepAlive = false;                   // indicate that it's stop time.
        interrupt();
        //synchronized( working ) {
        //    working.notifyAll();             // wake from working.wait above.
        //}
    }

    /**
     * Stop the thread's execution by interrupting it without marking the flag as not alive
     */
    public void stopExecution() {
        interrupt();
    }

    /**
     * Return my id
     */
    public int getID() {
        return (threadID);
    }

    /**
     * Return the resource for the specified key
     */
    public Object getResource(Object key) {
        return (objectCache.get(key));
    }

    /**
     * Return the resource for the specified key
     */
    public Object putResource(Object key, Object value) {
        return (objectCache.put(key, value));
    }

    /**
     * Clears the resource cache
     */
    public void clearResources() {
        objectCache.clear();
    }
}

