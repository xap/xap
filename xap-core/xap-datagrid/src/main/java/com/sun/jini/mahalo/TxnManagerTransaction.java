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

import com.gigaspaces.client.transaction.xa.GSServerTransaction;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.internal.utils.concurrent.UncheckedAtomicIntegerFieldUpdater;
import com.gigaspaces.lrmi.ILRMIProxy;
import com.gigaspaces.time.SystemTime;
import com.sun.jini.constants.TimeConstants;
import com.sun.jini.constants.TxnConstants;
import com.sun.jini.landlord.LeasedResource;
import com.sun.jini.logging.Levels;
import com.sun.jini.mahalo.log.ClientLog;
import com.sun.jini.mahalo.log.LogException;
import com.sun.jini.mahalo.log.LogManager;
import com.sun.jini.thread.TaskManager;
import com.sun.jini.thread.WakeupManager;

import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.core.transaction.*;
import net.jini.core.transaction.server.CrashCountException;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionConstants;
import net.jini.core.transaction.server.TransactionManager;
import net.jini.core.transaction.server.TransactionParticipant;
import net.jini.id.Uuid;
import net.jini.security.ProxyPreparer;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TxnManagerTransaction is a class which captures the internal representation of a transaction in
 * the TxnManagerImpl server.  This class is associated with a transaction id. The information
 * encapsulated includes the list of participants which have joined the transaction, the state of
 * the transaction, the crash.
 *
 * The user of a ParticipantHolder must make the association between an instance of a
 * ParticipantHolder and some sort of key or index.
 *
 * @author Sun Microsystems, Inc.
 */
class TxnManagerTransaction
        implements TransactionConstants, TimeConstants, LeasedResource {
    static final long serialVersionUID = -2088463193687796098L;
    private static final boolean _disableNewSpaceProxyRouter = Boolean.getBoolean("com.gigaspaces.client.router.disableNewRouter");

    /*
     * Table of valid state transitions which a
     * TransactionManager may make.
     *
     * This represents the following diagram from the
     * Jini(TM) Transaction Spec:
     *
     *  ACTIVE ----> VOTING ------->COMMITTED
     *     \           |
     *      \          |
     *       ---------------------->ABORTED
     *
     *
     *		    ACTIVE  VOTING  PREPARED  NOTCHANGED  COMMITTED  ABORTED
     * ----------------------------------------------------------------------
     * ACTIVE	    true     true   false     false       false       true
     * VOTING        false    true   false     false       true        true
     * PREPARED      false    false  false     false       false       false
     * NOTCHANGED    false    false  false     false       false       false
     * COMMITTED     false    false  false     false       true        false
     * ABORTED       false    false  false     false       false       true
     *
     * The table is indexed using the ordered pair
     * <current_state, next_state>.  A value of true means
     * that the transition is possible, while a false
     * means that the transition is not possible.
     *
     * Note:  Some rows are "{false, false, false, false}" as
     * 	     unused filler to account for the fact that the
     *	     TransactionManager's valid states are a subset
     *	     of the TransactionConstants.
     *
     *    <zero>
     *    ACTIVE = 1
     *    VOTING = 2
     *    PREPARED = 3
     *    NOTCHANGED = 4
     *    COMMITTED = 5
     *    ABORTED = 6
     */
    private static final boolean states[][] = {
    /* <zero>    */ {false, false, false, false, false, false, false},
    /* ACTIVE    */ {false, true, true, false, false, false, true},
	/* VOTING    */ {false, false, true, false, false, true, true},
	/* PREPARED  */ {false, false, false, false, false, false, false},
	/* NOTCHANGED*/ {false, false, false, false, false, false, false},
	/* COMMITTED */ {false, false, false, false, false, true, false},
	/* ABORTED   */ {false, false, false, false, false, false, true}};

    //as a sub manager (under XA0 ===============>
    private static final boolean subManagerStates[][] = {
   		/* <zero>    */ {false, false, false, false, false, false, false},
   		/* ACTIVE    */ {false, true, true, false, false, false, true},
   		/* VOTING    */ {false, false, true, true, false, true, true},
   		/* PREPARED  */ {false, false, false, true, false, true, true},
   		/* NOTCHANGED*/ {false, false, false, false, false, false, false},
   		/* COMMITTED */ {false, false, false, false, false, true, false},
   		/* ABORTED   */ {false, false, false, false, false, false, true}};


    /**
     * @serial
     */
    private static final int INITIAL_CAPACITY = 8;
    private static final float LOAD_FACTOR = 0.75f;
    private static final int SEGMENTS = 4;


    private HashMap<Integer, ParticipantHandle> _parts = null;

    //if xtn has 1 participant- its stored in singleHandle for optimizations 
    private ParticipantHandle _singleHandle;


    /**
     * @serial
     */
    private final ServerTransaction str;

    /**
     * @serial
     */
    private volatile int _trState = ACTIVE;

    private static final AtomicIntegerFieldUpdater<TxnManagerTransaction> _stateUpdater = UncheckedAtomicIntegerFieldUpdater.newUpdater(TxnManagerTransaction.class, "_trState");

    /**
     * @serial
     */
    private long expires;        //expiration time

    private boolean _leaseForEver;

    /**
     * @serial
     */
    private final LogManager logmgr;

    /**
     * "Parallelizing" the interaction between the manager and participants means using threads to
     * interact with participants on behalf of the manager. In the thread pool model, a TaskManager
     * provides a finite set of threads used to accomplish a variety of tasks. A Job encapsulates a
     * body of work which needs to be performed during the two-phase commit: preparing, committing
     * and aborting.  Each work item is broken into smaller pieces of work- interactions with a
     * single participant- each assigned to a task.
     *
     * When a transaction is committing, a PrepareJob is created and its tasks are scheduled.  After
     * completion, the PrepareJob's outcome is computed. Depending on the outcome, either an
     * AbortJob or CommitJob is scheduled.  When a transaction is aborted, an AbortJob is
     * scheduled.
     *
     * A caller may specify a timeout value for commit/abort. The timeout represents the length of
     * time a caller is willing to wait for participants to be instructed to roll-forward/back.
     * Should this timeout expire, a TimeoutExpiredException is thrown.  This causes the caller's
     * thread to return back to the caller.  Someone needs to finish contacting all the
     * participants.  This is accomplished by the SettlerTask.  SettlerTasks must use a different
     * thread pool from what is used by the various Job types, otherwise deadlock will occur.
     *
     * @serial
     */
    private final TaskManager threadpool;

    /**
     * @serial
     */
    private final WakeupManager wm;

    /**
     * @serial
     */
    private final TxnSettler settler;

    /**
     * @serial
     */
    private Job job;

    /**
     * @serial
     */
    private final Uuid uuid;

    /**
     * Interlock for the expiration time since lease renewal which set it may compete against lease
     * checks which read it.
     *
     * @serial
     */
    private final Object leaseLock = new Object();


    /**
     * Interlock for Jobs is needed since many threads on behalf of many clients can simultaneously
     * access or modify the Job associated with this transaction when when attempting to prepare,
     * roll forward or roll back participants.
     *
     * @serial
     */
    private Object jobLock;


    private final boolean _persistent;

    /**
     * Logger for operation related messages
     */
    private static final Logger operationsLogger =
            TxnManagerImpl.operationsLogger;

    /**
     * Logger for transaction related messages
     */
    private static final Logger transactionsLogger =
            TxnManagerImpl.transactionsLogger;

    private final boolean finer_op_logger;
    private final boolean finest_tr_logger;

    private final Object _externalXid;
    private volatile boolean _reenteredPreparedXid;

    private boolean _leaseRenewed;
    private long _leaseRenewedExtension;


    private final ConcurrentMap<String, IDirectSpaceProxy> _proxiesMap;  //for each cluster proxy by name 

    /**
     * Constructs a <code>TxnManagerTransaction</code>
     *
     * @param mgr        <code>TransactionManager</code> which owns this internal representation.
     * @param logmgr     <code>LogManager</code> responsible for recording COMMITTED and ABORTED
     *                   transactions to stable storage.
     * @param id         The transaction id
     * @param threadpool The <code>TaskManager</code> which provides the pool of threads used to
     *                   interact with participants.
     * @param settler    TxnSettler responsible for this transaction if unsettled.
     */
    TxnManagerTransaction(TransactionManager mgr, LogManager logmgr, long id,
                          TaskManager threadpool, WakeupManager wm, TxnSettler settler,
                          Uuid uuid, long lease, boolean persistent, Object externalXid, ConcurrentMap<String, IDirectSpaceProxy> proxiesMap) {
        if (logmgr == null)
            throw new IllegalArgumentException("TxnManagerTransaction: " +
                    "log manager must be non-null");
        if (mgr == null)
            throw new IllegalArgumentException("TxnManagerTransaction: " +
                    "transaction manager must be non-null");

        if (threadpool == null)
            throw new IllegalArgumentException("TxnManagerTransaction: " +
                    "threadpool must be non-null");

        if (wm == null)
            throw new IllegalArgumentException("TxnManagerTransaction: " +
                    "wakeup manager must be non-null");

        if (settler == null)
            throw new IllegalArgumentException("TxnManagerTransaction: " +
                    "settler must be non-null");

        if (uuid == null)
            throw new IllegalArgumentException("TxnManagerTransaction: " +
                    "uuid must be non-null");

        this.threadpool = threadpool;
        this.wm = wm;
        this.logmgr = logmgr;
        if (externalXid != null) {
            _externalXid = externalXid;
            str = new GSServerTransaction(mgr, externalXid, lease);
            str.id = id; //internal id for lease uuid etc
        } else {
            _externalXid = null;
            str = new ServerTransaction(mgr, id, lease);
        }
        this.settler = settler;
        this.uuid = uuid;
        _persistent = persistent;
        _proxiesMap = proxiesMap;

        //trstate = ACTIVE;  //this is implied since ACTIVE is initial state
        // Expires is set after object is created when the associated
        // lease is constructed.

        finer_op_logger = operationsLogger.isLoggable(Level.FINER);
        finest_tr_logger = transactionsLogger.isLoggable(Level.FINEST);

    }

    TxnManagerTransaction(TransactionManager mgr, LogManager logmgr, long id,
                          TaskManager threadpool, WakeupManager wm, TxnSettler settler,
                          Uuid uuid, long lease, boolean persistent) {
        this(mgr, logmgr, id,
                threadpool, wm, settler,
                uuid, lease, persistent, null, null);
    }

    /**
     * Convenience method which adds a given <code>ParticipantHandle</code> to the set of
     * <code>ParticpantHandle</code>s associated with this transaction.
     *
     * @param handle The added handle
     */
    synchronized void add(ParticipantHandle handle)
            throws InternalManagerException {
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "add", handle);
        }

        if (handle == null)
            throw new NullPointerException("ParticipantHolder: add: " +
                    "cannot add null handle");

        //NOTE: if the same participant re-joins, then that is
        //      fine.

        try {
            if (transactionsLogger.isLoggable(Level.FINEST)) {
                transactionsLogger.log(Level.FINEST,
                        "Adding ParticipantHandle: {0}", handle);
            }
            putInMap(handle.getPartionId(), handle);
        } catch (Exception e) {
            if (transactionsLogger.isLoggable(Level.SEVERE)) {
                transactionsLogger.log(Level.SEVERE,
                        "Unable to add ParticipantHandle", e);
            }
            throw new InternalManagerException("TxnManagerTransaction: " +
                    "add: " + e.getMessage());
        }
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                    "add");
        }
    }

    /**
     * Convenience method which allows the caller to modify the prepState associated with a given
     * <code>ParticipantHandle</code>
     *
     * @param handle The <code>ParticipantHandle</code> being modified
     * @param state  The new prepstate
     */
    synchronized void modifyParticipant(ParticipantHandle handle, int state) {
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "modifyParticipant", new Object[]{handle, new Integer(state)});
        }
        ParticipantHandle ph = null;

        if (handle == null)
            throw new NullPointerException("ParticipantHolder: " +
                    "modifyParticipant: cannot modify null handle");

        if (handle.equals(_singleHandle))
            ph = _singleHandle;
        else
            ph = getFromMap(handle.getPartionId());

        if (ph == null) {
            if (operationsLogger.isLoggable(Level.FINER)) {
                operationsLogger.exiting(
                        TxnManagerTransaction.class.getName(),
                        "modifyParticipant");
            }
//TODO - ignore??	    
            return;
        }

        ph.setPrepState(state);
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                    "modifyParticipant");
        }
    }


    /**
     * Changes the manager-side state of the transaction.  This method makes only valid state
     * changes and informs the caller if the change was successful. Calls to this method synchronize
     * around the manager-side state variable appropriately.
     *
     * @param int state the new desired state
     */
    boolean modifyTxnState(int state) {
        if (finer_op_logger) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "modifyTxnState", new Integer(state));
        }
        if (state != ACTIVE && state != VOTING && state != COMMITTED && state != ABORTED) {
            if (state != PREPARED || _externalXid == null) {
                throw new IllegalArgumentException("TxnManagerTransaction: " +
                        "modifyTxnState: invalid state");
            }
        }
        boolean result = false;

        while (true) {
            int curState = _trState;
            result = (_externalXid != null) ? subManagerStates[curState][state] : states[curState][state];

            if (result) {
                if (_stateUpdater.compareAndSet(this, curState, state))
                    break;
            } else
                break;
        }

        if (finer_op_logger) {
            operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                    "modifyTxnState", Boolean.valueOf(result));
        }
        return result;
    }

    /**
     * Implementation of the join method.
     *
     * @param part       The joining <code>TransactionParticpant</code>
     * @param crashCount The crashcount associated with the joining <code>TransactionParticipant</code>
     * @see net.jini.core.transaction.server.TransactionParticipant
     */
    public void
    join(TransactionParticipant part, long crashCount, ServerTransaction userXtnObject, int partitionId, String clusterName,
         IDirectSpaceProxy clusterProxy)
            throws CannotJoinException, CrashCountException, RemoteException {

        if (finer_op_logger) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "join", new Object[]{part, new Long(crashCount)});
        }
        //if the lease has expired, or the state is not
        //amenable there is no need to continue

        int state = getState();
        if (state != ACTIVE && !_reenteredPreparedXid)
            throw new CannotJoinException("not active");

        if ((state == ACTIVE) && !_leaseForEver && !ensureCurrent()) {
            doAbort(0);
            throw new CannotJoinException("Lease expired");
        }


        ParticipantHandle ph = null;
        //Create a ParticipantHandle for the new participant
        //and mark the transactional state as ACTIVE
        try {

            synchronized (this) {
                if (userXtnObject != null && _leaseRenewed)
                    updateLeaseInUserXtnIfNeeded(userXtnObject);

                if (_singleHandle != null && _singleHandle.getParticipant() == part) {
                    _singleHandle.setDisableDisjoin();

                    return; // already joined for this one
                }
                ph =
                        new ParticipantHandle(part, crashCount, null, _persistent, partitionId, clusterName, clusterProxy);

                System.out.println(">> singleHandler is null? " + (_singleHandle == null));
                if (_singleHandle == null) {
                    _singleHandle = ph;
                    return;
                }
                System.out.println(">> parts is null? " + (_parts == null));

                if (part instanceof ILRMIProxy) {
                    ILRMIProxy stub = (ILRMIProxy) part;
                    ph.setStubId(stub.getStubId());
                }

                if (_parts == null) {
                    _parts = new HashMap<Integer, ParticipantHandle>(2);
                    if (_singleHandle.getStubId() == null && _singleHandle.getParticipant() instanceof ILRMIProxy) {
                        ILRMIProxy stub = (ILRMIProxy) _singleHandle.getParticipant();
                        _singleHandle.setStubId(stub.getStubId());
                    }
                    putInMap(_singleHandle.getPartionId(), _singleHandle);

                    System.out.println(">> checking2... "+partitionId+", " + _singleHandle.getPartionId()+", " + ph.getStubId()+", "+_singleHandle.getStubId()+", ==? "+(ph.getStubId() == _singleHandle.getStubId())+", eq? "+(ph.getStubId().equals(_singleHandle.getStubId())));
//                    if (partitionId == _singleHandle.getClusterName().getPartionId() && !ph.getStubId().equals(_singleHandle.getStubId()))
                    if (isSamePartition(ph, _singleHandle)) {
                        throw new CannotJoinException("Same handler in join #1");
                    }
                }

                ParticipantHandle cur = getFromMap(ph.getPartionId());
                System.out.println(">> curr is null? " + (cur == null));
                if (cur == null) {
                    putInMap(ph.getPartionId(), ph);
                }  else {
                    System.out.println("("+(cur.getPartionId() == partitionId)+") - ("+(cur.getStubId() == ph.getStubId())+")");

                    if (isSamePartition(ph, cur)) {
                        System.out.println(">> aborting...");
                        throw new CannotJoinException("Same handler in join #2");
                    } else {
                        System.out.println(">> set disablejoin");
                        cur.setDisableDisjoin();
                    }
                }
            }

        } catch (InternalManagerException ime) {
            if (transactionsLogger.isLoggable(Level.SEVERE)) {
                transactionsLogger.log(Level.SEVERE,
                        "TransactionParticipant unable to join", ime);
            }
            throw ime;
        } catch (RemoteException re) {
            if (transactionsLogger.isLoggable(Level.FINEST)) {
                transactionsLogger.log(Level.FINEST,
                        "TransactionParticipant unable to be stored", re);
            }
            throw re;
        }
        if (finer_op_logger) {
            operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                    "join");
        }
    }

    private boolean isSamePartition(ParticipantHandle p1, ParticipantHandle p2) {
        if (p1.getPartionId() == p2.getPartionId() &&
                p1.getClusterName().equals(p2.getClusterName()) &&
                !p1.getStubId().equals(p2.getStubId())) {
            return true;
        } else {
            return false;
        }
    }


    private void updateLeaseInUserXtnIfNeeded(ServerTransaction userXtnObject) {
        _leaseRenewed = false;
        userXtnObject.setLease(_leaseRenewedExtension);
    }

    /**
     * Implementation of the disJoin method.- remove a participant that was joined for a first time.
     * called when a call to a participant returned empty so we can spare calling commit or abort on
     * it, usually used in embedded mahalo
     *
     * @param part The joining <code>TransactionParticpant</code>
     * @return true if participant disjoined
     * @see net.jini.core.transaction.server.TransactionParticipant
     */
    public boolean
    disJoin(TransactionParticipant part)
            throws RemoteException {

        if (finer_op_logger) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "disjoin", part);
        }
        //if the lease has expired, or the state is not
        //amenable there is no need to continue

        int state = getState();
        if (state != ACTIVE)
            throw new RuntimeException("not active");

        synchronized (this) {

            ParticipantHandle ph =
                    new ParticipantHandle(part, 0, null, _persistent);
            ParticipantHandle[] ps = parthandles();
            if (ps == null)
                return false;

            ILRMIProxy stub = (ILRMIProxy) part;
            ph.setStubId(stub.getStubId());


            for (ParticipantHandle p : ps) {
                if (p.equals(ph)) {
                    if (p.isDisableDisjoin())
                        return false;   //cant disjoin this participant

                    if (_parts != null) {
                        removeFromMap(p.getPartionId());
                        if (_parts.size() == 0)
                            _parts = null;
                    }
                    if (_singleHandle != null && _singleHandle.equals(ph)) {
                        _singleHandle = null;
                        ParticipantHandle[] psAfter = parthandles();
                        if (psAfter != null && psAfter.length > 0)
                            _singleHandle = psAfter[0];
                    }
                    return true;
                }
            }
            return false;

        }

    }

    /**
     * This method returns the state of the transaction. Since the purpose of the set of
     * ParticipantHolders is to associate a Transaction with a group of participants joined the
     * transaction, we would like to get the state of the transaction associated with the
     * aforementioned set.
     */
    public int getState() {
        return _trState;
    }


    /**
     * Commits the transaction. This initiates the two-phase commit protocol.  First, each
     * <code>net.jini.core.transaction.server.TransactionParticipant</code> in the set of
     * participants joined in the <code>net.jini.core.transaction.server.Transaction</code> is
     * instructed to vote and the votes are tallied. This is the first phase (prepare phase).
     *
     * Depending on the outcome of the votes, the transaction is considered committed or aborted.
     * Once commit/abort status is known, the participants are notified with a message to either
     * roll-forward (commit case) or roll-back (abort case).  This is the roll-phase.
     *
     * Since there may be a one-to-many relationship between a transaction and its participants,
     * <code>com.sun.jini.thread.TaskManager</code>s are used as a generic mechanism to provide the
     * threads needed to interact with the participants.
     */
    void commit(long waitFor)
            throws CannotCommitException, TimeoutExpiredException, RemoteException {
        commitImpl(waitFor, false /*prepareOnly*/);
    }


    int prepare(long waitFor)
            throws CannotCommitException, UnknownTransactionException, RemoteException {
        try {
            return commitImpl(waitFor, true /*prepareOnly*/);
        } catch (TimeoutExpiredException ex) {
            throw new CannotCommitException(getExternalXid().toString() + " " + ex.toString(), ex);
        }
    }


    int commitImpl(long waitFor, boolean prepareOnly)
            throws CannotCommitException, TimeoutExpiredException, RemoteException {

        if (finer_op_logger) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "commit", new Long(waitFor));
        }
        if (prepareOnly && waitFor != Long.MAX_VALUE)
            throw new UnsupportedOperationException();

        //If the transaction has already expired or the state
        //is not amenable, don't even try to continue
        int curstate = getState();
        if ((curstate == ACTIVE) && !_leaseForEver && (ensureCurrent() == false)) {
            doAbort(0);
            throw new CannotCommitException("Lease expired [ID=" + +getTransaction().id + "]");
        }

        if (curstate == ABORTED)
            throw new CannotCommitException("attempt to commit " +
                    "ABORTED transaction [ID=" + +getTransaction().id + "]");

        if (curstate == PREPARED && prepareOnly)
            return PREPARED;

        boolean use_light_prepareAndCommit = false;
        boolean use_light_Commit = false;
        if (!prepareOnly) {
            synchronized (this) {
                if (_parts == null && _singleHandle != null && !_disableNewSpaceProxyRouter && splitPrepareAndCommit(_singleHandle)) {
                    if (_proxiesMap.containsKey(_singleHandle.getClusterName()))
                        _singleHandle.setClusterProxy(_proxiesMap.get(_singleHandle.getClusterName()));
                }
                if (_parts == null && _singleHandle != null && (_disableNewSpaceProxyRouter || !splitPrepareAndCommit(_singleHandle))) {
                    if (_externalXid == null || curstate == ACTIVE)
                        use_light_prepareAndCommit = true;
                    else
                        use_light_Commit = (curstate == PREPARED && waitFor == Long.MAX_VALUE);
                } else if (_parts == null && _singleHandle != null) {
                    _parts = new HashMap<Integer, ParticipantHandle>(1);
                    putInMap(_singleHandle.getPartionId(), _singleHandle);
                }
            }
        }
        if (use_light_prepareAndCommit) {
            //single participant - optimize by cutting thru all
            lightPrepareAndCommit(_singleHandle);
            return COMMITTED;
        }
        if (use_light_Commit) {
            //single participant - optimize by cutting thru all
            lightCommit(_singleHandle);
            return COMMITTED;
        }
        //can we use light prepare ?
        boolean use_light_prepare = false;
        if (prepareOnly) {
            synchronized (this) {
                if (_parts == null && _singleHandle != null)
                    use_light_prepare = true;
            }
        }
        if (use_light_prepare) {
            //single participant embedded - optimize but cutting thru all
            return lightPrepare(_singleHandle);
        }


        long starttime = SystemTime.timeMillis();

        //Check to see if anyone joined the transaction.  Even
        //if no one has joined, at this point, attempt to
        //get to the COMMITTED state through valid state changes

        ParticipantHandle[] phs = parthandles();

        if (phs == null) {
            if (!modifyTxnState(VOTING))
                throw new CannotCommitException("attempt to commit " +
                        "ABORTED transaction [ID=" + +getTransaction().id + "]");

            if (modifyTxnState(COMMITTED))
                return COMMITTED;
            else
                throw new CannotCommitException("attempt to commit/prepare " +
                        "ABORTED transaction [ID=" + +getTransaction().id + "]");
        }
        boolean directPrepareAndCommit = false;
        if (jobLock == null)
            setJobLockIfNeed();

        try {

            long now = starttime;
            long transpired = 0;
            long remainder = 0;

            ClientLog log = logmgr.logFor(str.id);

            if (transactionsLogger.isLoggable(Level.FINEST)) {
                transactionsLogger.log(Level.FINEST,
                        "{0} TransactionParticipants have joined",
                        new Integer(phs.length));
            }

            //If commit is called after recovery, do not
            //log a CommitRecord since it already exists
            //exists in the Log file for this transaction.
            //Remember that a log is not invalidated until
            //after the transaction is committed.
            //
            //Only new occurrences of activities requiring
            //logging which happen after recovery should
            //be added to the log file.  So, we add records
            //for voting and roll forward/back activity for
            //ACTIVE participants.
            //
            //If the state cannot validly transition to VOTING,
            //it is either because someone already aborted or
            //committed.  Only throw an exception if  someone
            //has previously aborted.  In the case of a prior
            //commit, fall through and wait for the CommitJob
            //to complete.

            int oldstate = getState();
            Integer result = new Integer(ABORTED);
            Exception alternateException = null;


            //On an ACTIVE to VOTING transition, create
            //and schedule a Prepare or PrepareAndCommitJob.
            //If the state transition is VOTING to VOTING,
            //then the PrepareJob has already been created
            //in the past, so just fall through and wait
            //for it to complete.
            //
            //Only log the commit on ACTIVE to VOTING
            //transitions.

            if (modifyTxnState(VOTING)) {

                if (oldstate == ACTIVE)
                    log.write(new CommitRecord(phs));


                //preparing a participant can never override
                //the other activities (abort or commit),
                //so only set when the job is null.

                synchronized (jobLock) {

                    directPrepareAndCommit = waitFor == Long.MAX_VALUE && phs.length == 1 && !isExternalXid() && !prepareOnly;
                    if (job == null) {

                        if (phs.length == 1 && !isExternalXid() && !prepareOnly && !_disableNewSpaceProxyRouter && splitPrepareAndCommit(phs[0])) {
                            ParticipantHandle singleHandle = phs[0];
                            if (_proxiesMap.containsKey(singleHandle.getClusterName()))
                                singleHandle.setClusterProxy(_proxiesMap.get(singleHandle.getClusterName()));
                        }
                        if (phs.length == 1 && !isExternalXid() && !prepareOnly && (_disableNewSpaceProxyRouter || !splitPrepareAndCommit(phs[0])))
                            job = new
                                    PrepareAndCommitJob(
                                    str, threadpool, wm, log, phs[0], directPrepareAndCommit, _externalXid);
                        else {
                            directPrepareAndCommit = false;
                            job = new PrepareJob(str, threadpool, wm, log, phs, _externalXid, _proxiesMap);
                        }

                        if (directPrepareAndCommit) {
                            PrepareAndCommitJob pcj = (PrepareAndCommitJob) job;
                            pcj.doWork(null, phs[0]);
                        } else
                            job.scheduleTasks();
                    }
                }

                //Wait for the PrepareJob to complete.
                //PrepareJobs are given maximum time for
                //completion.  This is required in order to
                //know the transaction's completion status.
                //Remember that the timeout ONLY controls how
                //long the caller is willing to wait to inform
                //participants.  This means that a completion
                //status for the transaction MUST be computed
                //before consulting the timeout.
                //Timeout is ignored until completion status
                //is known.  If extra time is left, wait for
                //the remainder to inform participants.

                //We must explicitly check for Job type
                //because someone else could have aborted
                //the transaction at this point.


                synchronized (jobLock) {
                    if ((job instanceof PrepareJob) ||
                            (job instanceof PrepareAndCommitJob)) {
                        try {
                            if (job.isCompleted(Long.MAX_VALUE)) {
                                result = (Integer) job.computeResult();
                                if (result.intValue() == ABORTED &&
                                        job instanceof PrepareAndCommitJob) {
                                    PrepareAndCommitJob pj =
                                            (PrepareAndCommitJob) job;
                                    alternateException =
                                            pj.getAlternateException();
                                }
                                if (prepareOnly && (result == PREPARED || result == NOTCHANGED)) {
                                    modifyTxnState(result);
                                    return result;
                                }
                            }
                        } catch (JobNotStartedException jnse) {
                            //no participants voted, so do nothing
                            result = new Integer(NOTCHANGED);
                        } catch (ResultNotReadyException rnre) {
                            //consider aborted
                        } catch (JobException je) {
                            //consider aborted
                        }
                    }
                }
            } else {
                //Cannot be VOTING, so we either have
                //an abort or commit in progress.

                if (getState() == ABORTED)
                    throw new CannotCommitException("transaction ABORTED [ID=" + +getTransaction().id + "]");


                //If a CommitJob is already in progress
                //(the state is COMMITTED) cause a fall
                //through to the code which waits for
                //the CommitJob to complete.

                if (getState() == COMMITTED)
                    result = new Integer(COMMITTED);

                if (_externalXid != null && getState() == PREPARED)
                    result = new Integer(PREPARED);
            }

            if (transactionsLogger.isLoggable(Level.FINEST)) {
                transactionsLogger.log(Level.FINEST,
                        "Voting result: {0}",
                        TxnConstants.getName(result.intValue()));
            }

            switch (result.intValue()) {
                case NOTCHANGED:
                    break;

                case ABORTED:
                    now = SystemTime.timeMillis();
                    transpired = now - starttime;
                    remainder = waitFor - transpired;

                    if (remainder >= 0)
                        doAbort(remainder);
                    else
                        doAbort(0);

                    if (alternateException == null) {
                        throw new CannotCommitException(
                                "Unable to commit transaction [ID=" + +getTransaction().id + "]: "
                                        + getParticipantInfo(), getCommitExceptionCause(phs));
                    } else {
                        throw new RemoteException(
                                "Problem communicating with participant",
                                alternateException);
                    }

                case PREPARED:
                    //This entrypoint is entered if a PrepareJob
                    //tallied the votes with an outcome of
                    //PREPARED.  In order to inform participants,
                    //a CommitJob must be scheduled.
                    if (prepareOnly) {
                        modifyTxnState(PREPARED);
                        return PREPARED;
                    }

                    if (modifyTxnState(COMMITTED)) {
                        //TODO - log committed state record?
                        synchronized (jobLock) {
                            job = new CommitJob(str, threadpool, wm, log, phs, _externalXid);
                            job.scheduleTasks();
                        }
                    } else {
                        throw new CannotCommitException("attempt to commit " +
                                "ABORTED transaction [ID=" + +getTransaction().id + "]");
                    }

                    //Fall through to code with waits
                    //for CommitJob to complete.


                case COMMITTED:
                    //This entrypoint is the starting place for the code
                    //which waits for a CommitJob to complete and
                    //computes its resulting outcome. In addition,
                    //the wait time is enforced.  Should the wait time
                    //expire, a SettlerTask is scheduled on a thread
                    //pool.  The SettlerTask is needed to complete
                    //the commit (instruct participants to roll-forward)
                    //on behalf of the thread which exits when
                    //the TimeoutExpiredException is thrown.
                    //
                    //It is reached when...
                    //
                    // a) A commit was called on the same transaction twice.
                    //    When the thread comes in on the commit call, a
                    //    CommitJob already exists and the state is COMMITTED.
                    //
                    // b) The normal case where a PrepareJob was found to
                    //    have prepared the transaction and the tally of
                    //    votes resulted in a PREPARED outcome.  This causes
                    //    the state to be changed to COMMITTED and a
                    //    CommitJob to be created.
                    //
                    // c) A PrepareAndCommitJob has successfully prepared
                    //    a participant which rolled its changes forward.
                    //
                    //Note:  By checking to see if the CommitJob is already
                    //       present, this check allows us to use the same
                    //	 wait-for-CommitJob code for the regular
                    //       PREPARE/COMMIT, the COMMIT/COMMIT and
                    //       the PREPAREANDCOMMIT cases.

                    synchronized (jobLock) {
                        //A prepareAndCommitJob is done at this
                        //point since the TransactionParticipant
                        //would have instructed itself to roll
                        //forward.

                        if (job instanceof PrepareAndCommitJob) {
                            if (!modifyTxnState(COMMITTED))
                                throw new CannotCommitException("transaction " +
                                        "ABORTED [ID=" + +getTransaction().id + "]");
                            break;
                        }


                        //If the abort already arrived, then stop

                        if (job instanceof AbortJob)
                            throw new CannotCommitException("transaction " +
                                    "ABORTED [ID=" + +getTransaction().id + "]");
                    }

                    if (getState() != COMMITTED)
                        throw new
                                InternalManagerException("TxnManagerTransaction: " +
                                "commit: " + job + " got bad state: " +
                                TxnConstants.getName(result.intValue()));


                    now = SystemTime.timeMillis();
                    transpired = now - starttime;

                    boolean committed = false;

                    //If the commit is asynchronous then...
                    //
                    // a) check to see if the wait time has transpired
                    //
                    // b) If it hasn't, sleep for what's left from the wait time

                    try {
                        remainder = waitFor - transpired;
                        synchronized (jobLock) {
                            if (remainder <= 0 || !job.isCompleted(remainder)) {
   							 /*
   							  * Note - SettlerTask will kick off another Commit/Abort task for the same txn
   							  * which will try go through the VOTING->Commit states again.
   							  */
                                //TODO - Kill off existing task? Postpone SettlerTask?
                                if (_externalXid != null)
                                    settler.noteUnsettledTxn(_externalXid);
                                else
                                    settler.noteUnsettledTxn(str.id);

                                //derive the execption in commit if any
                                throwCannotCommitExceptionIfNeeded(phs);


                                throw new TimeoutExpiredException(
                                        "timeout expired", true);
                            } else {
                                result = (Integer) job.computeResult();
                                committed = true;
                            }
                            //derive the execption in commit if any
                            throwCannotCommitExceptionIfNeeded(phs);

                        }
                    } catch (ResultNotReadyException rnre) {
                        //this should not happen, so flag
                        //as an error.
                    } catch (JobNotStartedException jnse) {
                        //an error
                    } catch (JobException je) {
                        //an error
                    }

                    if (committed)
                        break;

                default:
                    throw new InternalManagerException("TxnManagerTransaction: " +
                            "commit: " + job + " got bad state: " +
                            TxnConstants.getName(result.intValue()));
            }

            //We don't care about the result from
            //the CommitJob
            log.invalidate();

        } catch (RuntimeException rte) {
            if (transactionsLogger.isLoggable(Level.FINEST)) {
                transactionsLogger.log(Level.FINEST,
                        "Problem committing transaction",
                        rte);
            }
            throw rte;
        } catch (LogException le) {
            if (transactionsLogger.isLoggable(Level.FINEST)) {
                transactionsLogger.log(Level.FINEST,
                        "Problem persisting transaction",
                        le);
            }
            throw new CannotCommitException("Unable to log [ID=" + +getTransaction().id + "]");
        }
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                    "commit");
        }
        return COMMITTED;
    }

    private void throwCannotCommitExceptionIfNeeded(ParticipantHandle[] phs) throws CannotCommitException {
        Map<Integer, Exception> participantsExceptions = new HashMap<Integer, Exception>();
        for (ParticipantHandle ph : phs)
        {
            if (ph.getCommitException() != null)
            {
                participantsExceptions.put(ph.getPartionId(), ph.getCommitException());
            }
        }
        if (!participantsExceptions.isEmpty()) {
            throw new CannotCommitDistributedException("some participants failed in commit after prepare [ID=" + getTransaction().id +"]: - risk of partial transaction", participantsExceptions);
        }
    }

    private Throwable getCommitExceptionCause(ParticipantHandle[] phs) {
        for (ParticipantHandle participantHandle : phs) {
            if (participantHandle.getCommitException() != null)
                return participantHandle.getCommitException();
        }
        return null;
    }

    private boolean splitPrepareAndCommit(
            ParticipantHandle participantHandle) {
        return participantHandle.getPartionId() >= 0 && participantHandle.getClusterProxy() == null;
    }

    //p-and-commit for a single  participant- just call    using same thread
    private void lightPrepareAndCommit(ParticipantHandle singleHandle)
            throws CannotCommitException, TimeoutExpiredException, RemoteException {
        try {
            if (modifyTxnState(VOTING)) {
                if (singleHandle.isSuitableForFailover()) {
                    prepareAndCommitPartitionWithEnabledFailover(singleHandle);
                } else {
                    TxnMgrProxy cur = (TxnMgrProxy) str.mgr;
                    int res;
                    if (_externalXid == null)
                        res = singleHandle.getParticipant().prepareAndCommit(cur.createLightProxy(), str.id);
                    else
                        res = ((IRemoteSpace) (singleHandle.getParticipant())).prepareAndCommit(cur.createLightProxy(), _externalXid);

                    if (res != COMMITTED)
                        throw new CannotCommitException(
                                "Unable to commit transaction: "
                                        + getParticipantInfo(), singleHandle.getCommitException());
                    modifyTxnState(res);
                }
            } else {
                if (getState() == COMMITTED)
                    return;
                throw new CannotCommitException();
            }
        } catch (UnknownTransactionException e) {
            throw new CannotCommitException(" reason=" + e, e);
        }
    }


    private void prepareAndCommitPartitionWithEnabledFailover(
            ParticipantHandle singleHandle) throws CannotCommitException {
        TxnMgrProxy cur = (TxnMgrProxy) str.mgr;
        int res = PrepareAndCommitJob.commitAndPreparePartitionWithEnabledFailover(singleHandle, cur, str.id, _externalXid);
        if (res != COMMITTED)
            throw new CannotCommitException(
                    "Unable to commit transaction: "
                            + getParticipantInfo(), singleHandle.getCommitException());
        modifyTxnState(COMMITTED);
    }

    // commit for a single participant- just call using same thread
    private void lightCommit(ParticipantHandle singleHandle)
            throws CannotCommitException, TimeoutExpiredException,
            RemoteException {
        try {
            if (singleHandle.isSuitableForCommitFailover())
                commitPartitionWithEnabledFailover(singleHandle);
            else {
                TxnMgrProxy cur = (TxnMgrProxy) str.mgr;
                if (_externalXid == null)
                    singleHandle.getParticipant()
                            .commit(cur.createLightProxy(), str.id);
                else
                    ((IRemoteSpace) (singleHandle.getParticipant())).commit(cur.createLightProxy(),
                            _externalXid);
                modifyTxnState(COMMITTED);
            }
        } catch (UnknownTransactionException e) {
            throw new CannotCommitException("[ID=" + str.id + "]: reason=" + e, e);
        }
    }

    //commit for a single  participant- just call  using same thread
    private void commitPartitionWithEnabledFailover(ParticipantHandle singleHandle)
            throws CannotCommitException, TimeoutExpiredException, RemoteException {

        TxnMgrProxy cur = (TxnMgrProxy) str.mgr;
        CommitJob.commitPartitionWithEnabledFailover(singleHandle, cur.createLightProxy(), str.id, _externalXid, 1 /*numPrepared*/);
        modifyTxnState(COMMITTED);
        if (singleHandle.getCommitException() != null)
            throw singleHandle.getCommitException();
    }


    //prepare for a single  participant- just call   using same thread- relevant for XA when mahalo is a sub-manager  
    private int lightPrepare(ParticipantHandle singleHandle)
            throws CannotCommitException, TimeoutExpiredException, RemoteException {
        TxnMgrProxy cur = (TxnMgrProxy) str.mgr;
        try {
            if (modifyTxnState(VOTING)) {
                singleHandle.setPrepared();
                int res;
                boolean proxyExists = false;
                if (_disableNewSpaceProxyRouter) {
                    singleHandle.setClusterProxy(null);
                    singleHandle.setClusterName(null);
                    singleHandle.setPartitionId(-1);
                }

                if (singleHandle.isNeedProxyInCommit()) {
                    if (_proxiesMap.containsKey(singleHandle.getClusterName())) {
                        singleHandle.setClusterProxy(_proxiesMap.get(singleHandle.getClusterName()));
                        proxyExists = true;
                    }
                }
                if (singleHandle.isSuitableForCommitFailover()
                        && !proxyExists
                        && !_proxiesMap.containsKey(singleHandle.getClusterName()))
                    _proxiesMap.putIfAbsent(singleHandle.getClusterName(),
                            singleHandle.getClusterProxy());
                if (!_disableNewSpaceProxyRouter) {// we dont have the cluster proxy for this partition
                    ExtendedPrepareResult eres = null;
                    if (_externalXid == null)
                        eres = (ExtendedPrepareResult) ((IRemoteSpace) singleHandle.getParticipant()).prepare(cur.createLightProxy(),
                                str.id,
                                singleHandle.isNeedProxyInCommit());
                    else
                        eres = (ExtendedPrepareResult) ((IRemoteSpace) singleHandle.getParticipant()).prepare(cur.createLightProxy(),
                                _externalXid,
                                singleHandle.isNeedProxyInCommit());
                    res = eres.getVote();
                    if (eres.getProxy() != null
                            && singleHandle.isNeedProxyInCommit()) {
                        singleHandle.setClusterProxy(eres.getProxy());
                        if (!_proxiesMap.containsKey(singleHandle.getClusterName()))
                            _proxiesMap.putIfAbsent(singleHandle.getClusterName(),
                                    singleHandle.getClusterProxy());
                    }
                } else {
                    if (_externalXid == null)
                        res = singleHandle.getParticipant()
                                .prepare(cur.createLightProxy(), str.id);
                    else
                        res = ((IRemoteSpace) (singleHandle.getParticipant())).prepare(cur.createLightProxy(),
                                _externalXid);
                }
                modifyTxnState(res);
                return res;
            } else {
                if (getState() == COMMITTED || getState() == PREPARED)
                    return getState();
                throw new CannotCommitException();
            }

        } catch (UnknownTransactionException e) {
            throw new CannotCommitException(" reason=" + e, e);
        }
    }


    /**
     * Aborts the transaction. This method attempts to set the state of the transaction to the
     * ABORTED state.  If successful, it then creates an AbortJob which schedules tasks executed on
     * a thread pool.  These tasks interact with the participants joined in this transaction to
     * inform them to roll back.
     *
     * @param waitFor Timeout value which controls how long, the caller is willing to wait for the
     *                participants joined in the transaction to be instructed to roll-back.
     * @see com.sun.jini.mahalo.AbortJob
     * @see com.sun.jini.mahalo.ParticipantTask
     * @see com.sun.jini.thread.TaskManager
     * @see net.jini.core.transaction.server.TransactionParticipant
     */
    void abort(long waitFor)
            throws CannotAbortException, TimeoutExpiredException {
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "abort", new Long(waitFor));
        }
        boolean use_light_abort = false;
        if (waitFor == Long.MAX_VALUE) {
            synchronized (this) {
                if (_parts == null && _singleHandle != null)
                    use_light_abort = true;
                ;
            }
        }
        if (use_light_abort) {
            //single participant embedded - optimize but cutting thru all
            lightAbort(_singleHandle);
            return;

        }
        if (jobLock == null)
            setJobLockIfNeed();

        long starttime = SystemTime.timeMillis();
        boolean directAbortCall = false;
   	 /*
   	  * Since lease cancellation process sets expiration to 0 
   	  * and then calls abort, can't reliably check expiration 
   	  * at this point.
   	  */
        //TODO - Change internal, lease logic to call overload w/o expiration check
        //TODO - Add expiration check to abort for external clients
        try {
            ParticipantHandle[] phs = parthandles();

            if (phs == null) {
                if (modifyTxnState(ABORTED))
                    return;
                else
                    throw new
                            CannotAbortException("Transaction already COMMITTED");
            }
            ClientLog log = logmgr.logFor(str.id);


            //When attempting to abort, if someone has already
            //committed the transaction, then throw an Exception.
            //
            //If an abort is possible, and you find that an AbortJob
            //is already in progress, let the existing AbortJob
            //proceed.
            //
            //If an abort is possible, but a PrepareJob is in progress,
            //go ahead an halt the PrepareJob and replace it with
            //an AbortJob.

            if (modifyTxnState(ABORTED)) {
                log.write(new AbortRecord(phs));

                synchronized (jobLock) {
                    directAbortCall = waitFor == Long.MAX_VALUE && phs.length == 1;
                    if (!(job instanceof AbortJob)) {
                        if (job != null) {
                            if (job.isDirectCall())
                                throw new CannotAbortException("direct call  on Transaction in progress");
                            job.stop();
                        }
                        job = new AbortJob(str, threadpool, wm, log, phs, directAbortCall, _externalXid);
                        if (job.isDirectCall()) {
                            AbortJob aj = (AbortJob) job;
                            aj.doWork(null, phs[0]);
                        } else
                            job.scheduleTasks();
                    }
                }
            } else {
                throw new CannotAbortException("Transaction already COMMITTED");
            }


            //This code waits for an AbortJob to complete and
            //computes its resulting outcome. In addition,
            //the wait time is enforced.  Should the wait time
            //expire, a SettlerTask is scheduled on a thread
            //pool.  The SettlerTask is needed to complete
            //the abort (instruct participants to roll-back)
            //on behalf of the thread which exits when
            //the TimeoutExpiredException is thrown.

            long now = SystemTime.timeMillis();
            long transpired = now - starttime;

            Integer result = new Integer(ACTIVE);
            boolean aborted = false;

            long remainder = waitFor - transpired;

            try {
                synchronized (jobLock) {
                    if (remainder <= 0 || !job.isCompleted(remainder)) {
                        if (_externalXid != null)
                            settler.noteUnsettledTxn(_externalXid);
                        else
                            settler.noteUnsettledTxn(str.id);
                        throw new TimeoutExpiredException(
                                "timeout expired", false);
                    } else {
                        result = (Integer) job.computeResult();
                        aborted = true;
                    }
                    //check for exception from abort participant
                    for (ParticipantHandle ph : phs) {
                        if (ph.getAbortException() != null)
                            throw ph.getAbortException();
                    }
                }
            } catch (ResultNotReadyException rnre) {
                //should not happen, so flag as error
            } catch (JobNotStartedException jnse) {
                //error
            } catch (JobException je) {
                if (_externalXid != null)
                    settler.noteUnsettledTxn(_externalXid);
                else
                    settler.noteUnsettledTxn(str.id);

                throw new TimeoutExpiredException("timeout expired", false);
            }


            if (!aborted)
                throw new InternalManagerException("TxnManagerTransaction: " +
                        "abort: AbortJob got bad state: " +
                        TxnConstants.getName(result.intValue()));

            log.invalidate();
        } catch (RuntimeException rte) {
            if (transactionsLogger.isLoggable(Level.SEVERE)) {
                transactionsLogger.log(Level.SEVERE,
                        "Problem aborting transaction",
                        rte);
            }
            throw new InternalManagerException("TxnManagerTransaction: " +
                    "abort: fatal error");
        } catch (LogException le) {
            if (transactionsLogger.isLoggable(Level.FINEST)) {
                transactionsLogger.log(Level.FINEST,
                        "Problem persisting transaction",
                        le);
            }
            throw new CannotAbortException("Unable to log");
        }
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                    "abort");
        }
    }


    private void setJobLockIfNeed() {
        synchronized (this) {
            if (jobLock == null)
                jobLock = new Object();
        }
    }


    //abort for a single embedded participant- just call  
    private void lightAbort(ParticipantHandle singleHandle)
            throws CannotAbortException, TimeoutExpiredException {
        TxnMgrProxy cur = (TxnMgrProxy) str.mgr;
        try {
            if (getState() == ABORTED)
                return;
            if (modifyTxnState(ABORTED)) {
                if (singleHandle.isSuitableForCommitFailover()) {
                    AbortJob.abortPartitionWithEnabledFailover(singleHandle, cur.createLightProxy(), str.id, _externalXid);
                } else {
                    if (_externalXid == null)
                        singleHandle.getParticipant().abort(cur.createLightProxy(), str.id);
                    else
                        ((IRemoteSpace) singleHandle.getParticipant()).abort(cur.createLightProxy(), _externalXid);
                }
                if (singleHandle.getAbortException() != null)
                    throw singleHandle.getAbortException();
            } else {
                throw new CannotAbortException("Transaction already COMMITTED");
            }
        } catch (UnknownTransactionException e) {
            // TODO Auto-generated catch block
            throw new CannotAbortException(" reason=" + e);
        } catch (RemoteException e) {
            // TODO Auto-generated catch block
            throw new CannotAbortException(" reason=" + e);
        }
    }


    public ServerTransaction getTransaction() {
        return str;
    }

    public long getExpiration() {
        if (finer_op_logger) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "getExpiration");
        }
        synchronized (leaseLock) {
            if (finer_op_logger) {
                operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                        "getExpiration", new Date(expires));
            }
            return expires;
        }
    }

    //the calling thread should lock this object
    public long getExpirationUnderThisLock() {
        return expires;
    }

    public void setExpiration(long newExpiration) {
        if (finer_op_logger) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "setExpiration", new Date(newExpiration));
        }
        synchronized (leaseLock) {
            setExpirationUnsafe(newExpiration);
            if (finer_op_logger) {
                operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                        "setExpiration");
            }
        }
    }

    public void setExpirationUnsafe(long newExpiration) {
        expires = newExpiration;
        _leaseForEver = expires == Long.MAX_VALUE;
    }


    public Uuid getCookie() {
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "getCookie");
        }
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                    "getCookie", uuid);
        }
        return uuid;
    }

    private void doAbort(long timeout) {
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "doAbort", new Long(timeout));
        }
        try {
            str.abort(timeout);
        } catch (RemoteException re) {
            //abort must have happened, so ignore
            if (transactionsLogger.isLoggable(Levels.HANDLED)) {
                transactionsLogger.log(Levels.HANDLED,
                        "Trouble aborting  transaction", re);
            }
        } catch (TimeoutExpiredException te) {
            //Swallow this because we really only
            //care about a scheduling a SettlerTask
            if (transactionsLogger.isLoggable(Levels.HANDLED)) {
                transactionsLogger.log(Levels.HANDLED,
                        "Trouble aborting  transaction", te);
            }
        } catch (TransactionException bte) {
            //If abort has problems, swallow
            //it because the abort must have
            //happened
            if (transactionsLogger.isLoggable(Levels.HANDLED)) {
                transactionsLogger.log(Levels.HANDLED,
                        "Trouble aborting  transaction", bte);
            }
        }
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                    "doAbort");
        }

    }

    synchronized boolean ensureCurrent() {
        if (finer_op_logger) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "ensureCurrent");
        }
        long useby = getExpiration();
        if (useby == Long.MAX_VALUE)
            return true;

        long cur = SystemTime.timeMillis();
        boolean result = false;

        if (useby > cur)
            result = true;
        if (finer_op_logger) {
            operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                    "ensureCurrent", Boolean.valueOf(result));
        }
        return result;
    }


    private synchronized ParticipantHandle[] parthandles() {
        if (_singleHandle != null && _parts == null)
            return new ParticipantHandle[]{_singleHandle};

        if ((_parts == null))
            return null;
        ArrayList<ParticipantHandle> vect = null;

        HashMap<Integer, ParticipantHandle> map = _parts;
        for (Map.Entry<Integer, ParticipantHandle> entry : map.entrySet()) {
            if (vect == null)
                vect = new ArrayList<ParticipantHandle>();
            vect.add(entry.getValue());
        }
        if (vect == null)

            if (transactionsLogger.isLoggable(Level.FINEST)) {
                transactionsLogger.log(Level.FINEST,
                        "Retrieved {0} participants",
                        new Integer(0));
            }

        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                    "parthandles");
        }
        return vect == null ? null : vect.toArray(new ParticipantHandle[vect.size()]);
    }


    private String getParticipantInfo() {
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "getParticipantInfo");
        }
        ParticipantHandle[] phs = parthandles();

        if (phs == null)
            return "No participants";

        if (transactionsLogger.isLoggable(Level.FINEST)) {
            transactionsLogger.log(Level.FINEST,
                    "{0} participants joined", new Integer(phs.length));
        }
        StringBuilder sb = new StringBuilder(phs.length + " Participants: ");

        int i = 0;
        for (ParticipantHandle ph : phs) {
            i++;
            sb.append(
                    "{" + i + ", "
                            + ph.getParticipant().toString() + ", "
                            + TxnConstants.getName(ph.getPrepState())
                            + "} ");
        }

        return sb.toString();
    }

    void restoreTransientState(ProxyPreparer preparer)
            throws RemoteException {
        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.entering(TxnManagerTransaction.class.getName(),
                    "restoreTransientState");
        }

        ParticipantHandle[] phs = parthandles();
        if (phs == null)
            return;
        int size = phs.length;

        ParticipantHandle[] handles = new ParticipantHandle[size];
        int j = 0;
        for (ParticipantHandle ph : phs) {
            handles[++j] = ph;

        }


        for (int i = 0; i < handles.length; i++) {
            handles[i].restoreTransientState(preparer);
            if (transactionsLogger.isLoggable(Level.FINEST)) {
                transactionsLogger.log(Level.FINEST,
                        "Restored transient state for {0}",
                        handles[i]);
            }
        }

        if (operationsLogger.isLoggable(Level.FINER)) {
            operationsLogger.exiting(TxnManagerTransaction.class.getName(),
                    "restoreTransientState");
        }
    }


    /**
     * Sends renew event to all transaction participants
     */
    public void renew(long extension) throws LeaseDeniedException, UnknownLeaseException {
        _leaseRenewed = true;
        _leaseRenewedExtension = extension;
        str.setLease(extension);


        ParticipantHandle[] phs = parthandles();

        if (phs == null)
            return;

        for (ParticipantHandle ph : phs) {
            renewParticipantLease(ph, extension);
        }
    }


    /**
     * @param participant
     * @param extension
     * @throws LeaseDeniedException
     * @throws UnknownLeaseException
     */
    private void renewParticipantLease(ParticipantHandle participant,
                                       long extension) {
        TransactionParticipant preParedParticipant = participant.getParticipant();

        try {
            preParedParticipant.renewLease(str.mgr, str.id, extension);
        } catch (Exception e) {
            //ignore disconnected participants or invalid leases
        }

    }


    public boolean isExternalXid() {
        return _externalXid != null;
    }

    public Object getExternalXid() {
        return _externalXid;
    }

    public void setReenteredPreparedXid()
            throws CannotCommitException {
        if (_externalXid == null)
            throw new UnsupportedOperationException();
        if (getState() == ABORTED)
            throw new CannotCommitException(" xtn is aborted");
        _reenteredPreparedXid = true;

        modifyTxnState(VOTING);
        modifyTxnState(PREPARED);

    }


    private ParticipantHandle getFromMap(Integer key) {
        return _parts.get(key);
    }

    private void putInMap(Integer key, ParticipantHandle handle) {
        _parts.put(key, handle);
    }

    private void removeFromMap(Integer key) {
        _parts.remove(key);
    }
}
