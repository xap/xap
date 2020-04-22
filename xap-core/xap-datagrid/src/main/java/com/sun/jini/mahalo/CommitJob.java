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

import com.gigaspaces.internal.client.spaceproxy.operations.CommitPreparedTransactionSpaceOperationRequest;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.j_spaces.core.OperationID;
import com.sun.jini.mahalo.log.ClientLog;
import com.sun.jini.thread.TaskManager;
import com.sun.jini.thread.WakeupManager;

import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.server.ExtendedTransactionManager;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionConstants;
import net.jini.core.transaction.server.TransactionParticipant;

import java.rmi.AccessException;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.MarshalException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.UnknownHostException;
import java.rmi.activation.ActivateFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a <code>Job</code> which interacts with a set of
 * <code>TransactionParticipant</code>s to inform them to roll forward changes associated with a
 * given <code>Transaction</code>.
 *
 * @author Sun Microsystems, Inc.
 * @see com.sun.jini.mahalo.Job
 * @see com.sun.jini.mahalo.ParticipantTask
 * @see net.jini.core.transaction.Transaction
 * @see net.jini.core.transaction.server.TransactionParticipant
 */
@com.gigaspaces.api.InternalApi
public class CommitJob extends Job implements TransactionConstants {
    ServerTransaction tr;
    ClientLog log;
    ParticipantHandle[] handles;
    int maxtries = Integer.MAX_VALUE;
    static final Logger logger = TxnManagerImpl.participantLogger;

    private final Object _xid;   //xid object 

    /**
     * Constructs an <code>CommitJob</code>
     *
     * @param tr      The <code>Transaction</code> whose participants will be instructed to
     *                roll-forward.
     * @param pool    The <code>TaskManager</code> which provides the threads used for interacting
     *                with participants.
     * @param log     The <code>ClientLog</code> used for recording recovery data.
     * @param handles The array of participants which will be contacted and informed to
     *                roll-forward.
     * @see com.sun.jini.thread.TaskManager
     * @see com.sun.jini.mahalo.log.ClientLog
     * @see net.jini.core.transaction.server.TransactionParticipant
     */
    public CommitJob(Transaction tr, TaskManager pool,
                     WakeupManager wm, ClientLog log,
                     ParticipantHandle[] handles, Object xid) {
        super(pool, wm, false);

        if (log == null)
            throw new IllegalArgumentException("CommitJob: CommitJob: " +
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
    }


    /**
     * The work to be performed by each <code>TaskManager.Task</code> is provided by the
     * <code>Job</code> that creates it. The work performed by a task belonging to the CommitJob
     * contacts a participant, instructs it to roll-forward and log appropriately.
     *
     * @param who   The task performing the work
     * @param param A parameter, of the task's choosing, useful in performing work.
     * @see com.sun.jini.mahalo.Job
     * @see com.sun.jini.thread.TaskManager.Task
     */
    Object doWork(TaskManager.Task who, Object param) {
        ParticipantHandle handle = (ParticipantHandle) param;
        TransactionParticipant par = null;

        //Check to see if participant has state associated
        //with it on account of being recovered from a log

        if (logger.isTraceEnabled()) {
            logger.trace(
                    "CommitJob:doWork committing handle: {0}", handle);
        }

        int vote = handle.getPrepState();

        switch (vote) {
            case COMMITTED:
            case NOTCHANGED:
            case ABORTED:
                return new Integer(vote);
        }

        //Instruct the TransactionParticipant to roll forward
        //after unpacking it and checking against the
        //max retry threshold.

        if (par == null)
            par = handle.getParticipant();

        //If you have exhausted the max retry threshold
        //stop, so that no further attempts are made.

        try {
            if (attempt(who) > maxtries) {
                return new Integer(COMMITTED);
            }
        } catch (JobException je) {
            return null;
        }

        //At this point, if participant is null, there
        //must be an error unpacking, so retry later
        if (par == null)
            return null;


        //Here we actually need to instruct the participant to
        //roll forward.  Note the RemoteException causes a
        //retry. Here we only log info for the cases
        //where a final outcome is available.

        Object response = null;

        try {
            int preparedParticipants = 0;
            for (int i = 0; i < handles.length; i++) {
                //ignore participants that have nothing to commit
                //count only participants of the same cluster
                if (handles[i].getPrepState() != NOTCHANGED && (handle.getClusterName() == handles[i].getClusterName() || handle.getClusterName() != null && handle.getClusterName().equals(handles[i].getClusterName())))
                    preparedParticipants++;
            }
            if (handle.isSuitableForCommitFailover()) {
                commitPartitionWithEnabledFailover(handle, tr.mgr, tr.id, _xid, preparedParticipants);
            } else {
                if (_xid == null)
                    par.commit(tr.mgr, tr.id, preparedParticipants);
                else
                    ((IRemoteSpace) par).commit(tr.mgr, _xid, preparedParticipants);
            }
            response = new Integer(COMMITTED);
        } catch (TransactionException bte) {
            //The participant doesn't have record of the
            //transaction, so it must have already rolled
            //forward.
            response = new Integer(COMMITTED);
        } catch (NoSuchObjectException nsoe) {
            //No definition for object in VM, so stop
            //and consider committed.
            response = new Integer(COMMITTED);
        } catch (ConnectException ce) {
            //failure setting up connection, so give
            //participant more time by retrying
            if (numberOfRetriesDueToConnectionExceptionExceeded(who))
                response = new Integer(COMMITTED);
        } catch (UnknownHostException uhe) {
            //could not resolve host for participant, so
            //stop and consider committed
            response = new Integer(COMMITTED);
        } catch (ConnectIOException cioe) {
            //multiplexed connection or cached
            //connection problem, give participant more time
            if (numberOfRetriesDueToConnectionExceptionExceeded(who))
                response = new Integer(COMMITTED);
        } catch (MarshalException me) {
            //cannot send parameters, so stop and consider done
            response = new Integer(COMMITTED);
        } catch (AccessException ae) {
            //Access error on registry or rmid consider done
            response = new Integer(COMMITTED);
        } catch (ActivateFailedException afe) {
            //Activatable Ref Stub couldn't activate
            //participant, so stop and consider done
            response = new Integer(COMMITTED);
        } catch (RemoteException re) {
            //Something happened with the network, so
            //return null to retry at a later time.
            response = new Integer(COMMITTED);
        } catch (RuntimeException rte) {
            //Something happened with the participant, so
            //stop and consider done
            response = new Integer(COMMITTED);
        }


        if (response != null) {
            handle.setPrepState(COMMITTED);
            try {
                log.write(new ParticipantCommitRecord(handle));
            } catch (com.sun.jini.mahalo.log.LogException le) {
                //the full package name used to disambiguate
                //the LogException
            }

            return response;
        }

        return null;
    }


    public static void commitPartitionWithEnabledFailover(ParticipantHandle handle, ExtendedTransactionManager mgr, long id, Object xid, int numPrepared) {
        try {
            //create request for the designated partition
            CommitPreparedTransactionSpaceOperationRequest request = null;
            OperationID operationID = handle.getClusterProxy().createNewOperationID();
            if (xid == null) {
                request = new CommitPreparedTransactionSpaceOperationRequest(mgr, id, handle.getPartionId(), numPrepared, operationID);
            } else {
                request = new CommitPreparedTransactionSpaceOperationRequest(mgr, xid, handle.getPartionId(), numPrepared, operationID);
            }
            // call execute on router giving the request
            handle.getClusterProxy().getProxyRouter().execute(request);
            request.getRemoteOperationResult().processExecutionException();
        } catch (Exception ex) {
            CannotCommitException ex1 = new CannotCommitException("commit failed [ID=" + id + "]: reason=" + ex, ex);
            handle.setCommitException(ex1);
        }
    }

    /**
     * Creates the <code>TaskManager.Task</code>s necessary to inform participants to roll-back.
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
        try {
            if (!isCompleted(0))
                throw new ResultNotReadyException("Cannot compute result " +
                        "since there are jobs pending");
        } catch (JobNotStartedException jnse) {
            throw new ResultNotReadyException("Cannot compute result since" +
                    " jobs were not created");
        }

        int tmp = 0;
        int count = 0;

        checkresults:
        for (int i = 0; i < results.length; i++) {
            tmp = ((Integer) results[i]).intValue();

            if (tmp == COMMITTED)
                count++;
        }

        if (logger.isTraceEnabled()) {
            logger.trace(
                    "CommitJob:computeResult {0} participants COMMITTED",
                    new Integer(count));
        }

        return new Integer(COMMITTED);
    }
}
