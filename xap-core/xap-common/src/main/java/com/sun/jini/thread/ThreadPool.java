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

import com.sun.jini.action.GetLongAction;

import java.security.AccessController;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThreadPool is a simple thread pool implementation of the Executor interface.
 *
 * A new task is always given to an idle thread, if one is available; otherwise, a new thread is
 * always created.  There is no minimum warm thread count, nor is there a maximum thread count
 * (tasks are never queued unless there are sufficient idle threads to execute them).
 *
 * New threads are created as daemon threads in the thread group that was passed to the ThreadPool
 * instance's constructor.  Each thread's name is the prefix NewThreadAction.NAME_PREFIX followed by
 * the name of the task it is currently executing, or "Idle" if it is currently idle.
 *
 * <p>This implementation uses the {@link Logger} named <code>com.sun.jini.thread.ThreadPool</code>
 * to log information at the following levels:
 *
 * <p><table summary="Describes what is logged by ThreadPool at various logging levels" border=1
 * cellpadding=5>
 *
 * <tr> <th> Level <th> Description
 *
 * <tr> <td> {@link Level#WARNING WARNING} <td> uncaught exception in worker thread
 *
 * </table>
 *
 * @author Sun Microsystems, Inc.
 **/
final class ThreadPool implements Executor {

    /**
     * how long a thread waits in the idle state before passing away
     */
    private static final long idleTimeout =        // default 5 minutes
            ((Long) AccessController.doPrivileged(new GetLongAction(
                    "com.sun.jini.thread.idleThreadTimeout", 300000)))
                    .longValue();

    private static final Logger logger =
            LoggerFactory.getLogger("com.sun.jini.thread.ThreadPool");

    /**
     * thread group that this pool's threads execute in
     */
    private final ThreadGroup threadGroup;

    /**
     * lock guarding all mutable instance state (below)
     */
    private final Object lock = new Object();

    /**
     * threads definitely available to take new tasks
     */
    private int idleThreads = 0;

    /**
     * queues of tasks to execute
     */
    private final LinkedList queue = new LinkedList();

    /**
     * Creates a new thread group that executes tasks in threads of the given thread group.
     */
    ThreadPool(ThreadGroup threadGroup) {
        this.threadGroup = threadGroup;
    }

    public void execute(Runnable runnable, String name) {
        Task task = new Task(runnable, name);
        synchronized (lock) {
            if (queue.size() < idleThreads) {
                queue.addLast(task);
                lock.notify();
                return;
            }
        }
        Thread t = (Thread) AccessController.doPrivileged(
                new NewThreadAction(threadGroup, new Worker(task), name, true));
        t.start();
    }

    /**
     * Task simply encapsulates a task's Runnable object with its name.
     */
    private static class Task {

        final Runnable runnable;
        final String name;

        Task(Runnable runnable, String name) {
            this.runnable = runnable;
            this.name = name;
        }
    }

    /**
     * Worker executes an initial task, and then it executes tasks from the queue, passing away if
     * ever idle for more than the idle timeout value.
     */
    private class Worker implements Runnable {

        private Task first;

        Worker(Task first) {
            this.first = first;
        }

        public void run() {
            Task task = first;
            first = null;

            while (true) {
                try {
                    task.runnable.run();
                } catch (Throwable t) {
                    logger.warn("uncaught exception", t);
                }
        /*
         * REMIND: What if the task changed this thread's
		 * priority? or context class loader?
		 */

                synchronized (lock) {
                    if (queue.isEmpty()) {
                        Thread.currentThread().setName(
                                NewThreadAction.NAME_PREFIX + "Idle");
                        idleThreads++;
                        try {
                            lock.wait(idleTimeout);
                        } catch (InterruptedException e) {
                            // ignore interrupts at this level
                        } finally {
                            idleThreads--;
                        }
                        if (queue.isEmpty()) {
                            break;        // timed out
                        }
                    }
                    task = (Task) queue.removeFirst();
                    Thread.currentThread().setName(
                            NewThreadAction.NAME_PREFIX + task.name);
                }
            }
            ;
        }
    }
}
