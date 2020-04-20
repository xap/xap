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
package com.sun.jini.mahalo;

import com.gigaspaces.time.SystemTime;
import com.sun.jini.thread.TaskManager;
import com.sun.jini.thread.TaskManager.Task;
import com.sun.jini.thread.WakeupManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A <code>Job</code> manages the division of work for a problem whose solution is obtained by
 * assembling partial results to original problem.
 *
 * @author Sun Microsystems, Inc.
 */
public abstract class Job {
    private static final int DEFAULT_CONNECTION_EXCEPTION_MAX_RETRIES = 3;
    private static final int CONNECTION_EXCEPTION_MAX_RETRIES = Integer.getInteger("com.gigaspaces.transaction.connection-exception-max-retries", DEFAULT_CONNECTION_EXCEPTION_MAX_RETRIES);

    final private TaskManager pool;
    final private WakeupManager wm;
    protected int pending = -1;
    protected Object[] results;
    private int[] attempts;
    protected Map tasks = new HashMap();  //used to maintain account
    //of the tasks for which
    //the job is responsible

    /* directCall no worker thread was activated.*/
    protected final boolean _directCall;

    static final private Logger logger = TxnManagerImpl.participantLogger;

    /**
     * Create the <code>Job</code> object giving it the <code>TaskManager</code> responsible for the
     * pool of threads which perform the necessary work.
     *
     * @param pool the <code>TaskManager</code> which provides the threads
     */
    public Job(TaskManager pool, WakeupManager wm, boolean directCall) {
        this.wm = wm;
        this.pool = pool;
        _directCall = directCall;
    }


    /**
     * Used by a task to do a piece of work and record the number of attempts.
     *
     * @param who   The task which is performing the work
     * @param param A parameter used in performing the work
     */
    boolean performWork(TaskManager.Task who, Object param)
            throws JobException {
        Integer tmp = null;

        synchronized (tasks) {
            tmp = (Integer) tasks.get(who);
        }

        if (tmp == null)
            throw new UnknownTaskException();

        int rank = tmp.intValue();

        synchronized (attempts) {
            attempts[rank]++;
        }

        Object result = doWork(who, param);
        if (result == null)
            return false;

        try {
            reportDone(who, result);
        } catch (UnknownTaskException e) {
        } catch (PartialResultException e) {
        } catch (JobException e) {
        }

        return true;
    }

    /**
     * Given a <code>TaskManager.Task</code>, this method returns the current number of attempts it
     * has made.
     *
     * @param who The task for which the number of attempts is inquired
     */
    int attempt(TaskManager.Task who) throws JobException {
        Integer tmp = null;

        synchronized (tasks) {
            tmp = (Integer) tasks.get(who);
        }

        if (tmp == null)
            throw new UnknownTaskException();

        int rank = tmp.intValue();

        synchronized (attempts) {
            return attempts[rank];
        }
    }


    /**
     * The work performed is implemented here. A null return value indicates failure while a
     * non-null return value indicates success and contains the result.
     *
     * @param who   The task performing the work
     * @param param A parameter used to do the work
     */
    abstract Object doWork(TaskManager.Task who, Object param)
            throws JobException;

    /**
     * Create the tasks required to compute all of the <code>PartialResult</code> objects necessary
     * for the solution to the original problem.
     */
    abstract TaskManager.Task[] createTasks();


    /**
     * Schedules tasks for execution
     */
    public void scheduleTasks() {
        TaskManager.Task[] tmp = createTasks();

        if (tmp != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Job:scheduleTasks with {} tasks", tmp.length);
            }

            results = new Object[tmp.length];
            attempts = new int[tmp.length];
            setPending(tmp.length);

            for (int i = 0; i < tmp.length; i++) {

                //Record the position if each
                //task for later use when assembling
                //the partial results

                synchronized (tasks) {
                    tasks.put(tmp[i], new Integer(i));
                    pool.add(tmp[i]);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Job:scheduleTasks added {} to thread pool", tmp[i]);
                    }
                    attempts[i] = 0;
                }
            }
        }
    }


    private synchronized void awaitPending(long waitFor) {
        if (pending < 0)
            return;

        if (pending == 0)
            return;

        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Job:awaitPending waiting for {} items", pending);
            }

            if (waitFor == Long.MAX_VALUE) {
                while (pending > 0) {
                    wait();
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                                "Job:awaitPending awoken");
                    }
                }
            } else {
                //When waiting for a given amount of time,
                //if notified, make sure that the desired
                //wait time has actually transpired.

                long start = SystemTime.timeMillis();
                long curr = start;

                while ((pending > 0) && ((curr - start) < waitFor)) {
                    wait(waitFor - (curr - start));
                    curr = SystemTime.timeMillis();
                }
            }
        } catch (InterruptedException ie) {
        }
    }

    private synchronized void setPending(int num) {
        pending = num;

        if (pending <= 0) {
            if (logger.isTraceEnabled()) {
                logger.trace("Job:setPending notifying, pending = {}", pending);
            }
            notifyAll();
        }
    }

    private synchronized void decrementPending() {
        pending--;

        if (pending <= 0) {
            if (logger.isTraceEnabled()) {
                logger.trace("Job:decrementPending notifying, pending = {}", pending);
            }
            notifyAll();
        }
    }


    /**
     * Returns a reference to the <code>TaskManager</code> which supplies the threads used to
     * executed tasks created by this <code>Job</code>
     */
    protected TaskManager getPool() {
        return pool;
    }

    /**
     * Returns a reference to the <code>WakeupManager</code> which provides the scheduling of tasks
     * created by this <code>Job</code>
     */
    protected WakeupManager getMgr() {
        return wm;
    }

    /*
     * Tasks which perform work on behalf of the <code>Job</code>
     * report in that they are done using this method.
     */
    protected void reportDone(TaskManager.Task who, Object param)
            throws JobException {
        if (param == null)
            throw new NullPointerException("param must be non-null");

        if (who == null)
            throw new NullPointerException("task must be non-null");

        Integer position = null;

        synchronized (tasks) {
            position = (Integer) tasks.get(who);
        }

        if (position == null)
            throw new UnknownTaskException();

        synchronized (results) {
            if (results[position.intValue()] == null) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Job:reportDone who = {}, param = {}", who, param);
                }
                results[position.intValue()] = param;
                decrementPending();
            } else {
                throw new PartialResultException("result already set");
            }
        }
    }


    /**
     * Check to see if the <code>Job</code> execution has completed.
     *
     * @param waitFor The amount of time the caller is willing to wait for the completion status to
     *                arrive.
     */
    public boolean isCompleted(long waitFor) throws JobException {
        //If nothing has started, the
        //task could not have completed.
        //Less than zero means initial value
        //and greater than zero means there
        //are outstanding tasks. In each of
        //these cases, the Job is not done.

        awaitPending(waitFor);

        synchronized (this) {
            if (pending == 0)
                return true;

            if (pending < 0)
                throw new JobNotStartedException("No jobs started");

            return false;
        }
    }


    /**
     * Generate the solution to the original problem. The subclass decides how it computes the final
     * outcome.
     */
    abstract Object computeResult() throws JobException;


    /**
     * Halt all of the work being performed  by the <code>Job</code>
     */
    public void stop() {
        Set s = tasks.keySet();
        Object[] vals = s.toArray();

        //Remove and interrupt all tasks

        for (int i = 0; i < vals.length; i++) {
            TaskManager.Task t = (TaskManager.Task) vals[i];
            pool.remove(t);
        }

        //Erase record of tasks, results and the
        //counting mechanism

        tasks = new HashMap();
        setPending(-1);
        results = null;
    }

    public boolean isDirectCall() {
        return _directCall;
    }

    protected boolean numberOfRetriesDueToConnectionExceptionExceeded(Task who) {
        try {
            return attempt(who) > CONNECTION_EXCEPTION_MAX_RETRIES;
        } catch (JobException e) {
            return true;
        }
    }

}
