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

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.logger.LogUtils;
import com.sun.jini.mahalo.log.ClientLog;
import com.sun.jini.thread.TaskManager;
import com.sun.jini.thread.WakeupManager;

import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionConstants;
import net.jini.core.transaction.server.TransactionParticipant;

import java.rmi.RemoteException;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a <code>com.sun.jini.mahalo.Job</code> which interacts with a set of
 * <code>net.jini.core.transaction.server.TransactionParticipant</code>s to inform them to vote.
 *
 * @author Sun Microsystems, Inc.
 * @see com.sun.jini.mahalo.Job
 * @see com.sun.jini.mahalo.ParticipantTask
 * @see net.jini.core.transaction.Transaction
 * @see net.jini.core.transaction.server.TransactionParticipant
 */
@com.gigaspaces.api.InternalApi
public class PrepareJob extends Job implements TransactionConstants {

    private static final boolean _disableNewSpaceProxyRouter = Boolean.getBoolean("com.gigaspaces.client.router.disableNewRouter");

    ServerTransaction tr;
    ClientLog log;
    ParticipantHandle[] handles;
    int maxtries = 5;
    /**
     * Logger for operations related messages
     */
    private static final Logger operationsLogger =
            TxnManagerImpl.operationsLogger;

    /**
     * Logger for persistence related messages
     */
    private static final Logger persistenceLogger =
            TxnManagerImpl.persistenceLogger;
    private final Object _xid;   //tid object 
    private final ConcurrentMap<String, IDirectSpaceProxy> _proxiesMap;  //for each cluster proxy by name

    /**
     * Constructs an <code>PrepareJob</code>
     *
     * @param tr      The <code>Transaction</code> whose participants will be instructed to vote
     * @param pool    The <code>TaskManager</code> which provides the threads used for interacting
     *                with participants.
     * @param log     The <code>ClientLog</code> used for recording recovery data.
     * @param handles The array of participants which will be contacted and informed to vote
     * @see com.sun.jini.thread.TaskManager
     * @see com.sun.jini.mahalo.log.ClientLog
     * @see net.jini.core.transaction.server.TransactionParticipant
     */
    public PrepareJob(Transaction tr, TaskManager pool,
                      WakeupManager wm, ClientLog log,
                      ParticipantHandle[] handles, Object xid, ConcurrentMap<String, IDirectSpaceProxy> proxiesMap) {
        super(pool, wm, false);

        if (log == null)
            throw new IllegalArgumentException("PrepareJob: PrepareJob: " +
                    "log is null");

        this.log = log;

        if (!(tr instanceof ServerTransaction))
            throw new IllegalArgumentException("PrepareJob: PrepareJob: " +
                    "must be a ServerTransaction");

        this.tr = (ServerTransaction) tr;

        if (handles == null)
            throw new IllegalArgumentException("PrepareJob: PrepareJob: " +
                    "must have participants");

        if (handles.length == 0)
            throw new IllegalArgumentException("PrepareJob: PrepareJob: " +
                    "must have participants");

        this.handles = handles;
        _xid = xid;
        _proxiesMap = proxiesMap;
    }


    /**
     * The work to be performed by each <code>TaskManager.Task</code> is provided by the
     * <code>Job</code> that creates it. The work performed by a task belonging to the AbortJob
     * contacts a participant, instructs it to vote and log appropriately.
     *
     * @param who   The task performing the work
     * @param param A parameter, of the task's choosing, useful in performing work.
     * @see com.sun.jini.mahalo.Job
     * @see com.sun.jini.thread.TaskManager.Task
     */
    Object doWork(TaskManager.Task who, Object param) {
        if (operationsLogger.isDebugEnabled()) {
            LogUtils.entering(operationsLogger, PrepareJob.class,"doWork", new Object[]{who, param});
        }
        ParticipantHandle handle = (ParticipantHandle) param;
        TransactionParticipant par = null;

        //check if a vote already exists because it was
        //recovered from the log. In this situation,
        //we do not need to log this info since it
        //exists in the log which was used for recovery...

        int vote = handle.getPrepState();

        switch (vote) {
            case COMMITTED:
            case NOTCHANGED:
            case ABORTED:
            case PREPARED:
                if (operationsLogger.isDebugEnabled()) {
                    LogUtils.exiting(operationsLogger, PrepareJob.class, "doWork", new Integer(vote));
                }
                return new Integer(vote);
        }

        //...otherwise, explicitly instruct the participant to
        //prepare after unpacking it and checking against the
        //max retry threshold

        if (par == null)
            par = handle.getParticipant();

        //If you have exhausted the max retry threshold
        //stop, so that no further attempts are made.

        try {
            if (attempt(who) > maxtries) {
                if (operationsLogger.isDebugEnabled()) {
                    LogUtils.exiting(operationsLogger, PrepareJob.class, "doWork",new Integer(ABORTED));
                }
                return new Integer(ABORTED);
            }
        } catch (JobException je) {
            if (operationsLogger.isDebugEnabled()) {
                LogUtils.exiting(operationsLogger, PrepareJob.class, "doWork", null);
            }
            return null;
        }


        //At this point, if participant is null, there
        //must be an error unpacking, so retry later
        if (par == null) {
            if (operationsLogger.isDebugEnabled()) {
                LogUtils.exiting(operationsLogger, PrepareJob.class, "doWork", null);
            }
            return null;
        }

        //Here we actually need to ask the participant to
        //prepare.  Note the RemoteException causes a
        //retry. Here we only log info for the cases
        //where a final outcome is available.

        Object response = null;

        try {
            if (_disableNewSpaceProxyRouter) {
                handle.setClusterProxy(null);
                handle.setClusterName(null);
                handle.setPartitionId(-1);
            }

            if (handle.isNeedProxyInCommit()) {
                if (_proxiesMap.containsKey(handle.getClusterName()))
                    handle.setClusterProxy(_proxiesMap.get(handle.getClusterName()));
            }
            int participantsCount = 0;

            for (int i = 0; i < handles.length; i++) {
                //count only participants of the same cluster
                if (handle.getClusterName() == handles[i].getClusterName() || handle.getClusterName() != null && handle.getClusterName().equals(handles[i].getClusterName()))
                    participantsCount++;
            }

            if (!_disableNewSpaceProxyRouter) {//we dont have the cluster proxy for this partition
                ExtendedPrepareResult res = null;
                if (_xid == null)
                    res = (ExtendedPrepareResult) ((IRemoteSpace) par).prepare(tr.mgr, tr.id, handle.isNeedProxyInCommit());
                else
                    res = (ExtendedPrepareResult) ((IRemoteSpace) par).prepare(tr.mgr, _xid, participantsCount, handle.isNeedProxyInCommit());
                vote = res.getVote();
                if (res.getProxy() != null && handle.isNeedProxyInCommit()) {
                    handle.setClusterProxy(res.getProxy());
                    if (!_proxiesMap.containsKey(handle.getClusterName()))
                        _proxiesMap.putIfAbsent(handle.getClusterName(), handle.getClusterProxy());
                }

            } else {
                if (_xid == null)
                    vote = par.prepare(tr.mgr, tr.id);
                else
                    vote = ((IRemoteSpace) par).prepare(tr.mgr, _xid, participantsCount);
            }
            response = new Integer(vote);
            handle.setPrepared();
        } catch (TransactionException bte) {
            vote = ABORTED;
            response = new Integer(vote);
        } catch (RemoteException re) {
            vote = ABORTED;
            response = new Integer(vote);
            handle.setPrepared(); //may have been prepared in remote
        } catch (RuntimeException rte) {
            vote = ABORTED;
            response = new Integer(vote);
            handle.setCommitException(new CannotCommitException("commit failed [ID=" + tr.id + "]: reason="
                    + rte, rte));
        }

        if (response != null) {
            handle.setPrepState(vote);
            try {
                log.write(new PrepareRecord(handle, vote));
            } catch (com.sun.jini.mahalo.log.LogException le) {
                //the full package name used to disambiguate
                //the LogException
                if (persistenceLogger.isWarnEnabled()) {
                    persistenceLogger.warn(
                            "Problem writing PrepareRecord.", le);
                }
//TODO - ignore?		
            }
            if (operationsLogger.isDebugEnabled()) {
                LogUtils.exiting(operationsLogger, PrepareJob.class, "doWork", response);
            }

            return response;
        }

        if (operationsLogger.isDebugEnabled()) {
            LogUtils.exiting(operationsLogger, PrepareJob.class, "doWork", null);
        }
        return null;
    }


    /**
     * Creates the <code>TaskManager.Task</code>s necessary to inform participants to vote.
     */
    TaskManager.Task[] createTasks() {
        TaskManager.Task[] tmp = new TaskManager.Task[handles.length];

        for (int i = 0; i < handles.length; i++) {
            tmp[i] =
                    new ParticipantTask(getPool(), getMgr(), this, handles[i]);
        }

        return tmp;
    }


    /**
     * Gathers partial results submitted by tasks and produces a single outcome.
     *
     * @see com.sun.jini.mahalo.Job
     */
    Object computeResult() throws JobException {
        if (operationsLogger.isDebugEnabled()) {
            LogUtils.entering(operationsLogger, PrepareJob.class, "computeResult");
        }
        try {
            if (!isCompleted(0))
                throw new ResultNotReadyException("Cannot compute result " +
                        "since there are jobs pending");
        } catch (JobNotStartedException jnse) {
            throw new ResultNotReadyException("Cannot compute result since" +
                    " jobs were not created");
        }

        int prepstate = NOTCHANGED;
        int tmp = 0;

        checkresults:
        for (int i = 0; i < results.length; i++) {
            tmp = ((Integer) results[i]).intValue();

            switch (tmp) {
                case NOTCHANGED:
                    //Does not affect the prepstate
                    break;

                case ABORTED:
                    //Causes all further checks to end
                    //while marking ABORTED. A single abort
                    //aborts the whole transaction.

                    prepstate = ABORTED;
                    break checkresults;

                case PREPARED:
                    //changes the state to PREPARED only
                    //if currently NOTCHANGED
                    if (prepstate == NOTCHANGED)
                        prepstate = PREPARED;
                    break;
            }
        }
        Integer result = new Integer(prepstate);
        if (operationsLogger.isDebugEnabled()) {
            LogUtils.exiting(operationsLogger, PrepareJob.class, "computeResult", result);
        }
        return result;
    }
}
