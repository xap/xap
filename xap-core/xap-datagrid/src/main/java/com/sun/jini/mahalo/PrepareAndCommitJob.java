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

import com.gigaspaces.internal.client.spaceproxy.operations.PrepareAndCommitTransactionSpaceOperationRequest;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.logger.LogUtils;
import com.j_spaces.core.OperationID;
import com.sun.jini.mahalo.log.ClientLog;
import com.sun.jini.thread.TaskManager;
import com.sun.jini.thread.WakeupManager;

import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionConstants;
import net.jini.core.transaction.server.TransactionManager;
import net.jini.core.transaction.server.TransactionParticipant;

import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * An implementation of a <code>Job</code> which interacts with a set of
 * <code>TransactionParticipant</code>s to inform them to vote and roll forward/back changes
 * associated with a given <code>Transaction</code>.
 *
 * @author Sun Microsystems, Inc.
 * @see com.sun.jini.mahalo.Job
 * @see com.sun.jini.mahalo.ParticipantTask
 * @see net.jini.core.transaction.Transaction
 * @see net.jini.core.transaction.server.TransactionParticipant
 */
@com.gigaspaces.api.InternalApi
public class PrepareAndCommitJob extends Job implements TransactionConstants {
    ServerTransaction tr;
    ClientLog log;
    ParticipantHandle handle;
    int maxtries = 5;

    /*
     * Field that holds the last received remote exception, if any.
     * Used as a flag for retry logic below.
     */
    private volatile RemoteException reCaught = null;

    /*
     * Flag used to indicate that client needs to be notified of a possible
     * indeterminate state.
     */
    private volatile boolean notifyClient = false;

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

    private final Object _xid;   //xid object 


    /**
     * Constructs a <code>PrepareAndCommitJob</code>.
     *
     * @param tr         The <code>Transaction</code> whose participants will be instructed to vote
     *                   and roll-forward/back.
     * @param pool       The <code>TaskManager</code> which provides the threads used for
     *                   interacting with participants.
     * @param log        The <code>ClientLog</code> used for recording recovery data.
     * @param handle     The array of participants which will be contacted and informed to vote and
     *                   roll-forward/back.
     * @param directCall no worker thread was activated.
     * @see com.sun.jini.thread.TaskManager
     * @see com.sun.jini.mahalo.log.ClientLog
     * @see net.jini.core.transaction.server.TransactionParticipant
     */
    public PrepareAndCommitJob(Transaction tr, TaskManager pool,
                               WakeupManager wm, ClientLog log,
                               ParticipantHandle handle, boolean directCall, Object tid) {
        super(pool, wm, directCall);

        if (log == null)
            throw new IllegalArgumentException("PrepareAndCommitJob: " +
                    "PrepareAndCommitJob: log is null");

        this.log = log;

        if (!(tr instanceof ServerTransaction))
            throw new IllegalArgumentException("PrepareAndCommitJob: " +
                    "PrepareAndCommitJob: " +
                    "must be a ServerTransaction");

        this.tr = (ServerTransaction) tr;

        if (handle == null)
            throw new IllegalArgumentException("PrepareAndCommitJob: " +
                    "PrepareJob: " +
                    "must have participants");

        this.handle = handle;

        if (_directCall)
            this.results = new Integer[1];

        _xid = tid;
    }


    /**
     * The work to be performed by each <code>TaskManager.Task</code> is provided by the
     * <code>Job</code> that creates it. The work performed by a task belonging to the CommitJob
     * contacts a participant, instructs it to vote, roll-forward/back and log appropriately.
     *
     * @param who   The task performing the work
     * @param param A parameter, of the task's choosing, useful in performing work.
     * @see com.sun.jini.mahalo.Job
     * @see com.sun.jini.thread.TaskManager.Task
     */
    Object doWork(TaskManager.Task who, Object param) {
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.entering(operationsLogger, PrepareAndCommitJob.class, "doWork", new Object[]{who, param});
        }

        Integer res = null;
        try {
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
                    if (operationsLogger.isLoggable(Level.FINER)) {
                        LogUtils.exiting(operationsLogger, PrepareAndCommitJob.class,
                                "doWork", new Integer(vote));
                    }
                    res = vote;
                    return res;
            }

            //...otherwise, explicitly instruct the participant to
            //prepare after unpacking it and checking against the
            //max retry threshold

            if (par == null)
                par = handle.getParticipant();

            //If you have exhausted the max retry threshold
            //stop, so that no further attempts are made.

            try {
                if (!_directCall && attempt(who) > maxtries) {
                    if (operationsLogger.isLoggable(Level.FINER)) {
                        LogUtils.exiting(operationsLogger, PrepareAndCommitJob.class, "doWork", new Integer(ABORTED));
                    }
                    res = ABORTED;
                    return res;
                }
            } catch (JobException je) {
                if (operationsLogger.isLoggable(Level.FINER)) {
                    LogUtils.exiting(operationsLogger, PrepareAndCommitJob.class,"doWork", null);
                }
                return null;
            }


            //At this point, if participant is null, there
            //must be an error unpacking, so retry later
            if (par == null) {
                if (operationsLogger.isLoggable(Level.FINER)) {
                    LogUtils.exiting(operationsLogger, PrepareAndCommitJob.class, "doWork", null);
                }
                return null;
            }

            //Here we actually need to ask the participant to
            //prepare.  Note the RemoteException causes a
            //retry. Here we only log info for the cases
            //where a final outcome is available.

            Integer response = null;

            try {
                TxnMgrProxy cur = (TxnMgrProxy) tr.mgr;
                boolean optimize = cur.isEmbeddedMgr();
                TransactionManager txnManager = optimize ? cur.createLightProxy() : tr.mgr;
                if (handle.isSuitableForFailover()) {
                    vote = commitAndPreparePartitionWithEnabledFailover(handle, txnManager, tr.id, _xid);
                } else {
                    //use a stubless proxy for single participant optimization
                    if (_xid == null) {
                        vote = par.prepareAndCommit(txnManager, tr.id);
                    } else {
                        IRemoteSpace rs = (IRemoteSpace) par;
                        vote = rs.prepareAndCommit(txnManager, _xid);
                    }
                }
                response = vote;
            } catch (UnknownTransactionException ute) {
                if (reCaught != null) {
                    notifyClient = true;
                }
                vote = ABORTED;
                response = vote;
            } catch (RemoteException re) {
                reCaught = re;
                vote = ABORTED;
                response = vote;
            } catch (RuntimeException rte) {
                vote = ABORTED;
                response = vote;
            }

            if (response != null) {
                handle.setPrepState(vote);
                try {
                    log.write(new PrepareAndCommitRecord(handle, vote));
                } catch (com.sun.jini.mahalo.log.LogException le) {
                    //the full package name used to disambiguate
                    //the LogException
                    if (persistenceLogger.isLoggable(Level.WARNING)) {
                        persistenceLogger.log(Level.WARNING,
                                "Problem writing PrepareAndCommitRecord.", le);
                    }
                    //TODO - ignore?
                }
                if (operationsLogger.isLoggable(Level.FINER)) {
                    LogUtils.exiting(operationsLogger, PrepareAndCommitJob.class,"doWork", response);
                }
                res = response;
                return res;
            }

            if (operationsLogger.isLoggable(Level.FINER)) {
                LogUtils.exiting(operationsLogger, PrepareAndCommitJob.class, "doWork", null);
            }

            return null;
        } finally {
            if (_directCall && res != null) {
                this.results[0] = res;
                pending = 0;
            }
        }
    }


    public static int commitAndPreparePartitionWithEnabledFailover(
            ParticipantHandle handle, TransactionManager mgr, long id,
            Object xid) {
        try {
            // create request for the designated partition
            PrepareAndCommitTransactionSpaceOperationRequest request;
            OperationID operationID = handle.getClusterProxy().createNewOperationID();
            if (xid == null) {
                request = new PrepareAndCommitTransactionSpaceOperationRequest(mgr,
                        id,
                        handle.getPartionId(),
                        operationID);
            } else {
                request = new PrepareAndCommitTransactionSpaceOperationRequest(mgr,
                        xid,
                        handle.getPartionId(),
                        operationID);
            }
            // call execute on router giving the request
            handle.getClusterProxy().getProxyRouter().execute(request);
            request.getRemoteOperationResult().processExecutionException();
            return request.getRemoteOperationResult().getState();
        } catch (Exception ex) {
            CannotCommitException ex1 = new CannotCommitException("commit failed [ID=" + id + "]: reason="
                    + ex,
                    ex);
            handle.setCommitException(ex1);
        }
        return ABORTED;
    }


    /**
     * Creates the <code>TaskManager.Task</code>s necessary to inform participants to vote and
     * roll-forward/back.
     */
    TaskManager.Task[] createTasks() {
        TaskManager.Task[] tmp = new TaskManager.Task[1];

        tmp[0] = new ParticipantTask(getPool(), getMgr(), this, handle);

        return tmp;
    }


    /**
     * Gathers partial results submitted by tasks and produces a single outcome.
     *
     * @see com.sun.jini.mahalo.Job
     */
    Object computeResult() throws JobException {
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.entering(operationsLogger, PrepareAndCommitJob.class, "computeResult");
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

        prepstate = ((Integer) results[0]).intValue();

        Integer result = new Integer(prepstate);
        if (operationsLogger.isLoggable(Level.FINER)) {
            LogUtils.exiting(operationsLogger, PrepareAndCommitJob.class,"computeResult", result);
        }
        return result;
    }

    /**
     * Simple accessor that returns the the exception to send back to the client.
     */
    Exception getAlternateException() {
        if (notifyClient)
            return reCaught;
        else
            return null;
    }

}
