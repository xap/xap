/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.sun.jini.thread;

import com.gigaspaces.logger.LogLevel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A task manager manages a single queue of tasks, and some number of worker threads.  New tasks are
 * added to the tail of the queue.  Each thread loops, taking a task from the queue and running it.
 * Each thread looks for a task by starting at the head of the queue and taking the first task (that
 * is not already being worked on) that is not required to run after any of the tasks that precede
 * it in the queue (including tasks that are currently being worked on). <p> This class uses the
 * {@link Logger} named <code>com.sun.jini.thread.TaskManager</code> to log information at the
 * following logging levels: <p> <table border=1 cellpadding=5 summary="Describes logging performed
 * by TaskManager at different logging levels"> <caption halign="center" valign="top"><b><code>
 * com.sun.jini.thread.TaskManager</code></b></caption> <tr><th>Level<th>Description <tr><td>{@link
 * Level#SEVERE SEVERE}<td> failure to create a worker thread when no other worker threads exist
 * <tr><td>{@link Level#WARNING WARNING}<td> exceptions thrown by {@link TaskManager.Task} methods,
 * and failure to create a worker thread when other worker threads exist </table>
 *
 * @author Sun Microsystems, Inc.
 */
@com.gigaspaces.api.InternalApi
public class TaskManager {

    /**
     * The interface that tasks must implement
     */
    public interface Task extends Runnable {
        /**
         * Return true if this task must be run after at least one task in the given task list with
         * an index less than size (size may be less then tasks.size()).  Using List.get will be
         * more efficient than List.iterator.
         *
         * @param tasks the tasks to consider.  A read-only List, with all elements instanceof
         *              Task.
         * @param size  elements with index less than size should be considered
         */
        boolean runAfter(List tasks, int size);
    }

    /**
     * Logger
     */
    protected static final Logger logger =
            Logger.getLogger("com.sun.jini.thread.TaskManager");

    /**
     * Active and pending tasks
     */
    protected final ArrayList<Task> tasks = new ArrayList<Task>();
    /**
     * Index of the first pending task; all earlier tasks are active
     */
    protected int firstPending = 0;
    /**
     * Read-only view of tasks
     */
    protected final List roTasks = Collections.unmodifiableList(tasks);
    /**
     * Active threads
     */
    protected final List<TaskThread> threads = new ArrayList<TaskThread>();
    /**
     * Maximum number of threads allowed
     */
    protected final int maxThreads;
    /**
     * Idle time before a thread should exit
     */
    protected final long timeout;
    /**
     * Threshold for creating new threads
     */
    protected final float loadFactor;
    /**
     * True if manager has been terminated
     */
    protected boolean terminated = false;

    private ClassLoader contextClassLoader;

    private String threadName = "task";

    private int retriesOnIdle = 1;

    private int threadPriority = Thread.NORM_PRIORITY;

    private StackTraceElement[] creationStackTrace;

    /**
     * Create a task manager with maxThreads = 10, timeout = 15 seconds, and loadFactor = 3.0.
     */
    public TaskManager() {
        this(10, 1000 * 15, 3.0f);
    }

    /**
     * Create a task manager.
     *
     * @param maxThreads maximum number of threads to use on tasks
     * @param timeout    idle time before a thread exits
     * @param loadFactor threshold for creating new threads.  A new thread is created if the total
     *                   number of runnable tasks (both active and pending) exceeds the number of
     *                   threads times the loadFactor, and the maximum number of threads has not
     *                   been reached.
     */
    public TaskManager(int maxThreads, long timeout, float loadFactor) {
        this(maxThreads, timeout, loadFactor, "task", 1);
    }

    public TaskManager(int maxThreads, long timeout, float loadFactor, String threadName, int retriesOnIdle) {
        this(maxThreads, timeout, loadFactor, threadName, retriesOnIdle, Thread.NORM_PRIORITY);
    }

    public TaskManager(int maxThreads, long timeout, float loadFactor, String threadName, int retriesOnIdle, int threadPriority) {
        if (logger.isLoggable(Level.FINEST)) {
            this.creationStackTrace = new Exception().getStackTrace();
        }
        this.maxThreads = maxThreads;
        this.timeout = timeout;
        this.loadFactor = loadFactor;
        this.threadName = "GS-" + threadName; // Set GigaSpaces Thread name prefix
        this.retriesOnIdle = retriesOnIdle;
        this.threadPriority = threadPriority;
        this.contextClassLoader = Thread.currentThread().getContextClassLoader();
    }

    /**
     * Add a new task if it is not equal to (using the equals method) to any existing active or
     * pending task.
     */
    public synchronized void addIfNew(Task t) {
        if (!tasks.contains(t))
            addInternal(t);
    }

    /**
     * Add a new task.
     */
    public synchronized void add(Task t) {
        addInternal(t);
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public String getThreadName() {
        return threadName;
    }

    // internal add, not under sync. Should be called within one
    private void addInternal(Task t) {
        tasks.add(t);
        if (logger.isLoggable(Level.FINEST)) {
            if (tasks.size() > 50) {
                StringBuffer sb = new StringBuffer();
                if (creationStackTrace != null) {
                    for (int i = 0; i < creationStackTrace.length; i++) {
                        sb.append(creationStackTrace[i]).append("\n");
                    }
                }
                logger.warning("Task [" + threadName + "] has more than 50 tasks, consider reconfiguring it. Creation Stack Trace: \n" + sb.toString());
            }
        }
        boolean poke = true;
        while (threads.size() < maxThreads && needThread()) {
            TaskThread th;
            try {
                th = new TaskThread();
                th.setPriority(threadPriority);
                th.setContextClassLoader(contextClassLoader);
                th.start();
            } catch (Throwable tt) {
                try {
                    LogLevel level = threads.isEmpty() ? LogLevel.SEVERE : LogLevel.WARNING;
                    level.log(logger, "thread creation exception", tt);
                } catch (Throwable ttt) {
                }
                break;
            }
            threads.add(th);
            poke = false;
        }
        if (poke &&
                threads.size() > firstPending &&
                !runAfter(t, tasks.size() - 1)) {
            notify();
        }
    }

    /**
     * Add all tasks in a collection, in iterator order.
     */
    public synchronized void addAll(Collection c) {
        for (Iterator iter = c.iterator(); iter.hasNext(); ) {
            addInternal((Task) iter.next());
        }
    }

    /**
     * Return true if a new thread should be created (ignoring maxThreads).
     */
    protected boolean needThread() {
        int bound = (int) (loadFactor * threads.size());
        int max = tasks.size();
        if (max < bound)
            return false;
        max--;
        if (runAfter(tasks.get(max), max))
            return false;
        int ready = firstPending + 1;
        if (ready > bound)
            return true;
        for (int i = firstPending; i < max; i++) {
            if (!runAfter(tasks.get(i), i)) {
                ready++;
                if (ready > bound)
                    return true;
            }
        }
        return false;
    }

    /**
     * Returns t.runAfter(i), or false if an exception is thrown.
     */
    private boolean runAfter(Task t, int i) {
        try {
            return t.runAfter(roTasks, i);
        } catch (Throwable tt) {
            try {
                logger.log(Level.WARNING, "Task.runAfter exception", tt);
            } catch (Throwable ttt) {
            }
            return false;
        }
    }

    /**
     * Remove a task if it is pending (not active).  Object identity (==) is used, not the equals
     * method.  Returns true if the task was removed.
     */
    public synchronized boolean removeIfPending(Task t) {
        return removeTask(t, firstPending);
    }

    /*
     * Remove a task if it is pending or active.  If it is active and not being
     * executed by the calling thread, interrupt the thread executing the task,
     * but do not wait for the thread to terminate.  Object identity (==) is
     * used, not the equals method.  Returns true if the task was removed.
     */
    public synchronized boolean remove(Task t) {
        return removeTask(t, 0);
    }

    /**
     * Remove a task if it has index >= min.  If it is active and not being executed by the calling
     * thread, interrupt the thread executing the task.
     */
    private boolean removeTask(Task t, int min) {
        for (int i = tasks.size(); --i >= min; ) {
            if (tasks.get(i) == t) {
                tasks.remove(i);
                if (i < firstPending) {
                    firstPending--;
                    for (int j = threads.size(); --j >= 0; ) {
                        TaskThread thread = threads.get(j);
                        if (thread.task == t) {
                            if (thread != Thread.currentThread())
                                thread.interrupt();
                            break;
                        }
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Interrupt all threads, and stop processing tasks.  Only getPending should be used
     * afterwards.
     */
    public synchronized void terminate() {

        if (terminated)
            return;

        terminated = true;
        Thread currentThread = Thread.currentThread();
        for (Thread thread : threads) {
            if (thread.equals(currentThread))
                continue;
            thread.interrupt();
        }
    }

    public synchronized boolean isTerminated() {
        return this.terminated;
    }

    /**
     * Return all pending tasks.  A new list is returned each time.
     */
    public synchronized List<Task> getPending() {
        List<Task> tc = (List<Task>) tasks.clone();
        for (int i = firstPending; --i >= 0; ) {
            tc.remove(0);
        }
        return tc;
    }

    /**
     * Returns the total number of tasks to be executed and currently executing.
     */
    public synchronized int getTotalTasks() {
        return tasks.size();
    }

    /**
     * Return the maximum number of threads to use on tasks.
     */
    public int getMaxThreads() {
        return maxThreads;
    }

    /**
     * @return the number of threads currently allocated (less or equal to getMaxThreads())
     * @since 12.3
     */
    public synchronized int getThreadCount() {
        return threads.size();
    }

    /**
     * @return the number of threads actively running a task
     * @since 12.3
     */
    public synchronized int getRunningThreadCount() {
        int count = 0;
        for (int i=0; i<threads.size(); i++) {
            if (threads.get(i).task != null) {
                ++count;
            }
        }
        return count;
    }

    private class TaskThread extends Thread {

        /**
         * The task being run, if any
         */
        public Task task = null;

        private int currentRetries = retriesOnIdle;

        public TaskThread() {
            super(threadName);
            setDaemon(true);
        }

        /**
         * Find the next task that can be run, and mark it taken by moving firstPending past it (and
         * moving the task in front of any pending tasks that are skipped due to execution
         * constraints). If a task is found, set task to it and return true.
         */
        private boolean takeTask() {
            int size = tasks.size();
            for (int i = firstPending; i < size; i++) {
                Task t = tasks.get(i);
                if (!runAfter(t, i)) {
                    if (i > firstPending) {
                        tasks.remove(i);
                        tasks.add(firstPending, t);
                    }
                    firstPending++;
                    task = t;
                    return true;
                }
            }
            return false;
        }

        @Override
        public void run() {
            while (true) {
                synchronized (TaskManager.this) {
                    if (terminated)
                        return;
                    if (task != null) {
                        for (int i = firstPending; --i >= 0; ) {
                            if (tasks.get(i) == task) {
                                tasks.remove(i);
                                firstPending--;
                                break;
                            }
                        }
                        task = null;
                        interrupted(); // clear interrupt bit
                    }
                    if (!takeTask()) {
                        try {
                            TaskManager.this.wait(timeout);
                        } catch (InterruptedException e) {
                        }
                        if (terminated || !takeTask()) {
                            if (--currentRetries <= 0) {
                                threads.remove(this);
                                return;
                            }
                            continue;
                        }
                    }
                }
                //we run the task, reset retries
                currentRetries = retriesOnIdle;
                try {
                    task.run();
                } catch (Throwable t) {
                    try {
                        if (Thread.currentThread().isInterrupted() || t instanceof InterruptedException) {
                            boolean isTerminated = false;
                            synchronized (TaskManager.this) {
                                isTerminated = terminated;
                            }

                            logger.log(Level.FINER,
                                    (isTerminated ? "TaskManager was terminated - all tasks interrupted. " : "")
                                            + threadName + " ran [" + task + "] but was interrupted. ", t);
                        } else {
                            logger.log(Level.WARNING, threadName + " ran [" + task + "] and caught an exception.", t);
                        }
                    } catch (Throwable logException) {
                        logger.log(Level.SEVERE, "Ran [" + task + "] but caught exception: " + t + "\nAttempt to log caused: "
                                , logException);
                    }
                }
            }
        }


    }
}
