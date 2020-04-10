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

import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.time.SystemTime;
import com.sun.jini.config.Config;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import java.text.DateFormat;
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Queue of timed tasks.  Each task implements {@link Runnable}. Events can either be executed in
 * the queue's thread or in their own thread. <p> A task is an object that implements
 * <code>Runnable</code>.  It is scheduled by invoking {@link #schedule(long, Runnable,
 * WakeupManager.ThreadDesc) schedule} with a time at which it should be run.  When that time
 * arrives (approximately) the task will be pulled off the queue and have its {@link Runnable#run
 * run} method invoked.  <p>
 *
 * A <code>schedule</code> request can specify a {@link WakeupManager.ThreadDesc}, which will define
 * the parameters of a thread to be created to run the <code>Runnable</code>.  You can specify the
 * group, whether the thread is a daemon thread, and the priority. Additionally you can use a
 * subclass of <code>WakeupManager.ThreadDesc</code> and override the {@link
 * WakeupManager.ThreadDesc#thread thread} method to further customize thread creation. <p>
 *
 * When a task is scheduled, a {@link WakeupManager.Ticket} is returned that can be used to cancel
 * the event if desired. <p>
 *
 * The queue requires its own thread, whose parameters can be defined via a <code>ThreadDesc</code>
 * if desired. The queue's thread will be started when the first task is scheduled. If the queue
 * becomes empty the thread will be terminated after a <a href=#queueThreadTimeout>configurable
 * delay</a>. The thread will be re-started if a new task is scheduled. <p>
 *
 * While it is theoretically possible to obtain the queue's thread and interrupt it, the results of
 * doing so are undefined. If a client wishes to stop the queue's thread the client should either
 * remove all the tasks or call {@link #stop}. Note, calling <code>stop</code> will cause future
 * <code>schedule</code> calls to fail with an <code>IllegalStateException</code>. <p>
 *
 * <a name="ConfigEntries"> <code>WakeupManager</code> supports the <code>queueThreadTimeout</code>
 * configuration entry, with the component <code>com.sun.jini.thread.WakeupManager</code>.
 *
 * <a name="queueThreadTimeout"> <table summary="Describes the queueThreadTimeout configuration
 * entry" border="0" cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1">
 * <code>queueThreadTimeout</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> <code>long</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> 30,000 milliseconds
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> How long, in
 * milliseconds, the queue's thread will be left running if there are no scheduled tasks. Must be a
 * non-negative long value. This configuration entry is consulted when the
 * <code>WakeupManager</code> is initially created.
 *
 * </table> <p>
 *
 * This class uses the {@link Logger} named <code>com.sun.jini.thread.WakeupManager</code> to log
 * information at the following logging levels: <p>
 *
 * <table border=1 cellpadding=5 summary="Describes logging performed by WakeupManager at different
 * logging levels">
 *
 * <tr> <th> Level <th> Description
 *
 * <tr> <td> SEVERE <td> exceptions thrown when we attempt to create the queue's thread
 *
 * <tr> <td> WARNING <td> exceptions thrown by the run methods of tasks, by the
 * <code>ThreadDesc</code>'s of tasks, or if the queue's thread is interrupted
 *
 * <tr> <td> FINEST <td> how many milliseconds until the next event and when the queue's thread is
 * stopped or started
 *
 * </table>
 *
 * @author Sun Microsystems, Inc.
 * @see java.lang.Runnable
 */
@com.gigaspaces.api.InternalApi
public class WakeupManager {
    /**
     * Component we pull configuration entries from and our logger name
     */
    private final static String COMPONENT_NAME =
            "com.sun.jini.thread.WakeupManager";

    /**
     * Default value for <code>queueThreadTimeout</code>
     */
    private final static long DEFAULT_QUEUE_THREAD_TIMEOUT = 30000;

    /**
     * If there are no registered tasks number of milliseconds to wait before killing the kicker
     * thread
     */
    private final long queueThreadTimeout;

    /**
     * The queue. Also the object we use for locking, multi-threaded access to all the other fields
     * is arbitrated by synchronizing on this object.
     */
    private final SortedSet contents = new java.util.TreeSet();

    /**
     * <code>ThreadDesc</code> we use to create kicker threads
     */
    private final ThreadDesc kickerDesc;

    /**
     * The Runnable for the queue's thread
     */
    private final Kicker kicker = new Kicker();

    /**
     * Next tie breaker ticket
     */
    private long nextBreaker = 0;

    /**
     * First item in contents
     */
    private Ticket head = null;

    /**
     * The queue's thread
     */
    private Thread kickerThread;

    /**
     * <code>true</code> if we have been stopped.
     */
    private boolean dead = false;

    /**
     * <code>DataFormat</code> used by {@link Ticket} to format its <code>toString</code> return
     * value.
     */
    private static DateFormat dateFmt =
            DateFormat.getTimeInstance(DateFormat.LONG);

    /**
     * Logger for this class and nested classes
     */
    private static final Logger logger = LoggerFactory.getLogger(COMPONENT_NAME);

    /**
     * Description of a future thread.
     *
     * @see WakeupManager#schedule
     * @see WakeupManager#WakeupManager(WakeupManager.ThreadDesc)
     */
    public static class ThreadDesc {
        private final ThreadGroup group;    // group to create in
        private final boolean daemon;        // create as daemon?
        private final int priority;        // priority

        /**
         * Equivalent to
         * <pre>
         *     ThreadDesc(null, false)
         * </pre>
         */
        public ThreadDesc() {
            this(null, false);
        }

        /**
         * Equivalent to
         * <pre>
         *     ThreadDesc(group, deamon, Thread.NORM_PRIORITY)
         * </pre>
         */
        public ThreadDesc(ThreadGroup group, boolean daemon) {
            this(group, daemon, Thread.NORM_PRIORITY);
        }

        /**
         * Describe a future thread that will be created in the given group, deamon status, and
         * priority.
         *
         * @param group    The group to be created in.  If <code>null</code>, the thread will be
         *                 created in the default group.
         * @param daemon   The thread will be a daemon thread if this is <code>true</code>.
         * @param priority The thread's priority.
         * @throws IllegalArgumentException if priority is not in between {@link Thread#MIN_PRIORITY}
         *                                  and {@link Thread#MAX_PRIORITY}
         */
        public ThreadDesc(ThreadGroup group, boolean daemon, int priority) {
            if (priority < Thread.MIN_PRIORITY ||
                    priority > Thread.MAX_PRIORITY) {
                throw new IllegalArgumentException("bad value for priority:" +
                        priority);
            }

            this.group = group;
            this.daemon = daemon;
            this.priority = priority;
        }

        /**
         * The {@link ThreadGroup} the thread will be created in.
         *
         * @return the {@link ThreadGroup} the thread will be created in.
         */
        public ThreadGroup getGroup() {
            return group;
        }

        /**
         * Returns <code>true</code> if the the thread will be daemon thread, returns
         * <code>false</code> otherwise.
         *
         * @return <code>true</code> if the the thread will be daemon thread, returns
         * <code>false</code> otherwise.
         */
        public boolean isDaemon() {
            return daemon;
        }

        /**
         * The priority the thread should be created with.
         *
         * @return the priority the thread should be created with.
         */
        public int getPriority() {
            return priority;
        }

        /**
         * Create a thread for the given runnable based on the values in this object. May be
         * overridden to give full control over creation of thread.
         *
         * @return a thread to run <code>r</code>, unstarted
         */
        public Thread thread(Runnable r) {
            String name = "JiniWakeupManager";
            Thread thr = getGroup() == null ? new GSThread(r, name) : new GSThread(getGroup(), r, name);
            thr.setDaemon(isDaemon());
            thr.setPriority(getPriority());
            return thr;
        }

        public String toString() {
            return "[" + getGroup() + ", " + isDaemon() + ", "
                    + getPriority() + "]";
        }
    }

    /**
     * A ticket that can be used for cancelling a future task.  It describes the task itself as
     * well. The {@link WakeupManager#newTicket WakeupManager.newTicket} method can be used by
     * subclasses of <code>WakeupManager</code> to create new <code>Ticket</code> instances.
     */
    public static class Ticket implements Comparable {
        /**
         * When the task should occur.
         */
        public final long when;
        /**
         * The task object to be executed
         */
        public final Runnable task;
        /**
         * The <code>ThreadDesc</code>, or <code>null</code> if none.
         */
        public final ThreadDesc desc;

        /**
         * Tie beaker used when two tickets have the same value for when
         */
        private final long breaker;

        private Ticket(long when, Runnable task, ThreadDesc threadDesc,
                       long breaker) {
            if (task == null)
                throw new NullPointerException("task not specified");
            this.when = when;
            this.task = task;
            this.desc = threadDesc;
            this.breaker = breaker;
        }

        public String toString() {
            return dateFmt.format(new Long(when)) + "(" + when + ")" + ", "
                    + task.getClass().getName() + ", " + desc;
        }

        public boolean equals(Object o) {
            if (!(o instanceof Ticket))
                return false;

            final Ticket that = (Ticket) o;

            return that.when == when && that.breaker == breaker;
        }

        public int hashCode() {
            return (int) breaker;
        }

        public int compareTo(Object o) {
            final Ticket that = (Ticket) o;

            final long whenDiff = when - that.when;
            if (whenDiff > 0)
                return 1;
            else if (whenDiff < 0)
                return -1;
            else {
                final long breakerDiff = breaker - that.breaker;

                if (breakerDiff > 0)
                    return 1;
                else if (breakerDiff < 0)
                    return -1;
                else
                    return 0;
            }
        }
    }

    /**
     * Create a new <code>WakeupManager</code>. Equivalent to.
     * <pre>
     *     WakeupManager(new ThreadDesc())
     * </pre>
     *
     * @see WakeupManager.ThreadDesc
     */
    public WakeupManager() {
        this(new ThreadDesc());
    }

    /**
     * Create a new <code>WakeupManager</code>.  The thread used for timing will be created
     * according to the provided <code>ThreadDesc</code>.
     *
     * @throws NullPointerException if desc is null
     */
    public WakeupManager(ThreadDesc desc) {
        if (desc == null)
            throw new NullPointerException("desc must be non-null");

        kickerDesc = desc;
        queueThreadTimeout = DEFAULT_QUEUE_THREAD_TIMEOUT;
    }

    /**
     * Create a new <code>WakeupManager</code>.  The thread used for timing will be created
     * according to the provided <code>ThreadDesc</code>. Optionally pass a configuration to control
     * various implementation specific behaviors.
     *
     * @throws ConfigurationException if if an exception occurs while retrieving an item from the
     *                                given <code>Configuration</code> object
     * @throws NullPointerException   if either argument is null
     */
    public WakeupManager(ThreadDesc desc, Configuration config)
            throws ConfigurationException {
        if (desc == null)
            throw new NullPointerException("desc must be non-null");

        kickerDesc = desc;
        queueThreadTimeout = Config.getLongEntry(config, COMPONENT_NAME,
                "queueThreadTimeout", DEFAULT_QUEUE_THREAD_TIMEOUT,
                0, Long.MAX_VALUE);
    }

    /**
     * Create a new ticket with the specified values for when the task should be run, what task
     * should be run, and what sort of thread the task should be run in.
     *
     * @param when       when the task should run, an absolute time
     * @param task       what task should be run
     * @param threadDesc if non-<code>null</code> the object to use to create the thread the task
     *                   should be run in, if <code>null</code> the task should be run in the
     *                   manager's thread.
     * @throws NullPointerException if task is <code>null</code>
     */
    protected Ticket newTicket(long when, Runnable task, ThreadDesc threadDesc) {
        synchronized (contents) {
            return new Ticket(when, task, threadDesc, nextBreaker++);
        }
    }

    /**
     * Schedule the given task for the given time.  The task's <code>run</code> method will be
     * executed synchronously in the queue's own thread, so it should be brief or it will affect
     * whether future events will be executed at an appropriate time.
     *
     * @throws NullPointerException  if <code>task</code> is <code>null</code>
     * @throws IllegalStateException if the manager has been stopped
     */
    public Ticket schedule(long when, Runnable task) {
        return schedule(when, task, null);
    }

    /**
     * Schedule the given task for the given time, to be run in a thread. When the time comes, a new
     * thread will be created according to the <code>ThreadDesc</code> object provided.  If
     * <code>threadDesc</code> is <code>null</code>, this is equivalent to the other form of
     * <code>schedule</code>.
     *
     * @throws NullPointerException  if <code>task</code> is <code>null</code>
     * @throws IllegalStateException if the manager has been stopped
     */
    public Ticket schedule(long when, Runnable task, ThreadDesc threadDesc) {
        synchronized (contents) {
            if (dead)
                throw new IllegalStateException(
                        "trying to add task to stopped WakeupManager");

            Ticket t = newTicket(when, task, threadDesc);
            contents.add(t);

            if (kickerThread == null) {
                logger.trace("starting queue's thread");

                try {
                    final Thread thread = kickerDesc.thread(kicker);
                    thread.start();

                    // Only set once we know start worked
                    kickerThread = thread;
                } catch (Throwable tt) {
                    try {
                        logger.error(
                                "queue thread creation exception", tt);
                    } catch (Throwable ttt) {
                        // don't let a problem in logging kill the thread
                    }
                }
            }

            // need to call checkHead (even if we just (re)created the
            // kickerThread), because that is how head gets set (note,
            // this is ok to call even if thread creation failed)
            checkHead();

            return t;
        }
    }

    /**
     * Cancel the given ticket.
     */
    public void cancel(Ticket t) {
        synchronized (contents) {
            if (dead) return;

            contents.remove(t);
            checkHead();
        }
    }

    /**
     * Cancel all tickets.
     */
    public void cancelAll() {
        synchronized (contents) {
            if (dead) return;

            contents.clear();
            checkHead();
        }
    }


    /**
     * Called whenever we change contents to update head and see if we need to wake up the queue
     * thread. Assumes the caller holds the lock on contents.
     */
    private void checkHead() {
        assert Thread.holdsLock(contents);
        final Ticket oldHead = head;

        if (contents.isEmpty())
            head = null;
        else
            head = (Ticket) contents.first();

        if (head == oldHead) return;

        // New first event (including possibly no events), run
        // needs to wake up and change its sleep time.
        contents.notifyAll();
    }

    /**
     * Return whether the queue is currently empty.
     */
    public boolean isEmpty() {
        synchronized (contents) {
            return (contents.isEmpty());
        }
    }

    /**
     * Stop executing.
     */
    public void stop() {
        synchronized (contents) {
            contents.clear();
            kickerThread = null;
            head = null;
            dead = true;
            contents.notifyAll();
        }
    }

    /**
     * The kicker work.  This is what sleeps until the time of the next event.
     */
    private class Kicker implements Runnable {
        public void run() {
        /* Set when contents is empty to control when the kicker will
         * exit. Long.MIN_VALUE used as flag value to indicate
	     * kickerExitTime is invalid
	     */
            long kickerExitTime = Long.MIN_VALUE;

            while (true) {
                final Ticket ticketToRun;

                synchronized (contents) {
                    while (true) {
                        if (dead)
                            return;

                        final long now = SystemTime.timeMillis();
                        final long timeToNextEvent;

                        if (contents.isEmpty()) {
                            if (kickerExitTime == Long.MIN_VALUE) {
                                kickerExitTime = now + queueThreadTimeout;

                                if (kickerExitTime < 0) {
                                    // overflow
                                    kickerExitTime = Long.MAX_VALUE;
                                }
                            }

                            // Since contents is empty the next event is exit
                            timeToNextEvent = kickerExitTime - now;

                            if (timeToNextEvent <= 0) {
                                // been idle long enough, depart

				/* $$$ Do this in a finally block for the run?
				 * so no mater how this thread ends kickerThread
				 * get set to null?
				 */
                                kickerThread = null;

                                logger.trace(
                                        "stopping queue's thread");
                                return;
                            }
                        } else { // contents is non-empty
                            kickerExitTime = Long.MIN_VALUE;
                            timeToNextEvent = head.when - now;

                            if (timeToNextEvent <= 0) {
                                // The head's time has come, consume and
                                // break out of inner loop to run it.

                                ticketToRun = head;
                                contents.remove(head);
                                checkHead();
                                break;
                            }
                        }

                        if (logger.isTraceEnabled()) {
                            logger.trace("timeToNextEvent:{0}",
                                    (timeToNextEvent == Long.MAX_VALUE ?
                                            "Long.MAX_VALUE" :
                                            Long.toString(timeToNextEvent)));
                        }

                        assert timeToNextEvent > 0;

                        try {
                            contents.wait(timeToNextEvent);
                        } catch (InterruptedException e) {
			    /* This should never happen, our thread is
			     * private to WakeupManager and tasks
			     * calling Thread.currentThread().interrupt() is
			     * decidedly anti-social. Log, but keep on
			     * going.
			     */

                            try {
                                logger.warn(
                                        "Attempt to interrupt Queue's thread");
                            } catch (Throwable t) {
                                // ignore
                            }

			    /* This loop already deals with wait returning
			     * early for no reason, so going to the top
			     * of the loop is ok here - if there are no
			     * new tasks and we are not dead we will 
			     * just calc a new value for timeToNextEvent
			     */
                        }

			/* Something has changed or the time has arived
			 * for action, don't know which, go back to the 
			 * the top of the inner loop to figure out what to
			 * do next
			 */
                    }
                }

                // Run the task outside of the lock
                if (ticketToRun.desc == null) {
                    // ... in this thread
                    try {
                        ticketToRun.task.run();
                    } catch (Throwable e) {
                        try {
                            logger.warn("Runnable.run exception", e);
                        } catch (Throwable t) {
                            // don't let a problem in logging kill the thread
                        }
                    }
                } else {
                    // ... in its own thread
                    try {
                        ticketToRun.desc.thread(ticketToRun.task).start();
                    } catch (Throwable t) {
                        try {
                            logger.warn(
                                    "task thread creation exception", t);
                        } catch (Throwable tt) {
                            // don't let a problem in logging kill the thread
                        }
                    }
                }
            }
        }
    }
}


