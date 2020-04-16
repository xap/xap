/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.j_spaces.core.client;

import com.gigaspaces.client.transaction.xa.GSServerTransaction;
import com.gigaspaces.client.transaction.xa.GSXid;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.j_spaces.core.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.log.JProperties;
import com.sun.jini.mahalo.TxnMgrProxy;

import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.CannotAbortException;
import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.TimeoutExpiredException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.ExtendedTransactionManager;
import net.jini.core.transaction.server.TransactionConstants;
import net.jini.core.transaction.server.TransactionManager;
import net.jini.core.transaction.server.TransactionParticipant;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * An instance of this class represents a XAResource implementation
 *
 * @author Guy Korland
 * @version 4.5
 **/

public class XAResourceImpl
        implements XAResource, ActionListener {
    //recover related- call only once per proxy
    private static final Object _recoverLock = new Object();
    private static final Hashtable<ISpaceProxy, Xid[]> _recoverRes = new Hashtable<ISpaceProxy, Xid[]>();

    private final int DEFAULT_TIMEOUT;
    protected final ExtendedTransactionManager m_txnManger;
    private volatile int m_timeout; // in seconds
    final protected Hashtable<Xid, Xid> _activeEmptyTransactions;
    final private Hashtable<Xid, Transaction.Created> _suspendedXtns;
    final private UUID _rmid;
    final private static long COMMIT_ABORT_DEFAULT_TIMEOUT = 10 * 60 * 1000; //10 min'
    final private boolean _delegatedXa;
    final private boolean _resourcePerSingleTxn;
    volatile boolean _relevantTx; //relevant only if _perSingleTxnResource is true


    final private static Logger _logger =
            LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_XA);

    final private static boolean failOnInvalidRollback =
            Boolean.parseBoolean(System.getProperty(SystemProperties.FAIL_ON_INVALID_ROLLBACK, Boolean.TRUE.toString()));

    protected ISpaceProxy _proxy;

    /**
     * Ctor
     *
     * @param txnManger transaction manger, for used inside GS.
     */
    public XAResourceImpl(TransactionManager txnManger, IJSpace proxy, boolean delegatedXa, boolean resourcePerSingleTxn) {
        if (_logger.isDebugEnabled()) {
            _logger.debug("XAResourceImpl:Constructor(), the LocalTransactionManager is: " +
                    txnManger + ", ActionMaker is: " + proxy + " thread=" + Thread.currentThread().getId() + " delegatedXa=" + delegatedXa + " resourcePerSingleTxn=" + resourcePerSingleTxn);
        }
        _rmid = UUID.randomUUID();
        _activeEmptyTransactions = new Hashtable<Xid, Xid>();
        _suspendedXtns = new Hashtable<Xid, Transaction.Created>();
        m_txnManger = (ExtendedTransactionManager) txnManger;
        _proxy = (ISpaceProxy) proxy;
        _proxy.setActionListener(this);
        String containerName = null;
        String spaceName = null;
        _delegatedXa = delegatedXa;
        _resourcePerSingleTxn = resourcePerSingleTxn;
        if (_resourcePerSingleTxn)
            _relevantTx = true;

        if (_recoverRes.get(_proxy) == null) {
            synchronized (_recoverLock) {
                if (_recoverRes.get(_proxy) == null) {
                    Xid[] xr = new Xid[0];
                    if (_logger.isInfoEnabled()) {
                        _logger.info("XAResourceImpl:constructor geting recover info rmid=" + getRmid() + " thread=" + Thread.currentThread().getId() + " proxy=" + _proxy);
                    }
                    try {
                        xr = recoverImpl(0, true);
                    } catch (Exception ex) {
                    }
                    _recoverRes.put(_proxy, xr);
                }
            }
        }
        try {
            containerName = proxy.getContainerName();
            spaceName = proxy.getName();
        } catch (Exception e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(
                        "The XA resource default timeout " + Integer.MAX_VALUE +
                                "(msec) will be used since the following exception thrown:" +
                                e.toString(), e);
            }
        }

        //if exception thrown so use default values for timeout
        if (containerName == null || spaceName == null) {
            DEFAULT_TIMEOUT = Integer.MAX_VALUE;
            m_timeout = DEFAULT_TIMEOUT; // in seconds
        } else {
            String fullSpaceName =
                    JSpaceUtilities.createFullSpaceName(containerName, spaceName);

            DEFAULT_TIMEOUT =
                    Integer.parseInt(JProperties.getSpaceProperty(fullSpaceName,
                            Constants.StorageAdapter.XARESOURCE_TIMEOUT,
                            String.valueOf(Integer.MAX_VALUE)));

            m_timeout = DEFAULT_TIMEOUT; // in seconds
        }
    }

    public XAResourceImpl(TransactionManager txnManger, IJSpace proxy) {
        this(txnManger, proxy, false, false);
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#getTransactionTimeout()
     */
    public int getTransactionTimeout() throws XAException {
        return m_timeout;
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#setTransactionTimeout(int)
     */
    public boolean setTransactionTimeout(int timeout) throws XAException {
        if (timeout < 0)
            throw new XAException(XAException.XAER_INVAL);
        m_timeout = timeout == 0 ? DEFAULT_TIMEOUT : timeout;
        if (_logger.isDebugEnabled()) {
            _logger.debug("XAResourceImpl:setTransactionTimeout() set to  " + timeout + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
        }
        return true;
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#isSameRM(javax.transaction.xa.XAResource)
     */
    public boolean isSameRM(XAResource xares) throws XAException {
        boolean res = false;
        XAResourceImpl other = null;
        if (_resourcePerSingleTxn && !_relevantTx) { // signal TM (atomikos) resource can be purged
            if (_logger.isTraceEnabled())
                _logger.debug("tx is not relevant,rmid " + _rmid);
            throw new XAException("Not relevant TX!");
        }


        try {
            if (xares == this) {
                res = true;
            } else if (xares instanceof XAResourceImpl) {
                other = (XAResourceImpl) xares;

                //res = _rmid.equals(other.getRmid());
                res = other.m_txnManger.equals(m_txnManger) &&
                        (_proxy.equals(other._proxy));
            }
        } catch (Exception e) {
            // false
        } finally {
            if (_logger.isTraceEnabled()) {
                if (other != null)
                    _logger.trace("XAResourceImpl:isSameRM(), the XAResource is: " + xares + " our=" + _rmid + " other=" + other.getRmid() + " result=" + res + " thread=" + Thread.currentThread().getId());
                else
                    _logger.trace("XAResourceImpl:isSameRM(),othger is NULL  the XAResource is: " + xares + " our=" + _rmid + " result=" + res + " thread=" + Thread.currentThread().getId());

            }


        }
        return res;
    }

    public UUID getRmid() {
        return _rmid;
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#recover(int)
     */
    public Xid[] recover(int flag) throws XAException {
        return
                recoverImpl(flag, false);

    }

    private Xid[] recoverImpl(int flag, boolean fromInitialize) throws XAException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("XAResourceImpl:recover() called, flag: " + flag + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " fromInitialize=" + fromInitialize);
        }
        Xid[] res = null;
        Xid[] res_cons = null;
        if (!fromInitialize) {
            res_cons = _recoverRes.get(_proxy);
            if (res_cons.length == 0) {
                _logger.debug("XAResourceImpl:recover() returned empty, flag: " + flag + " xtns=" + res_cons.length + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
                return res_cons;
            } else {
                _logger.debug("XAResourceImpl:recover() advanced to recheck num=" + res_cons.length + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
            }
        }
        try {
            switch (flag) {
                case TMSTARTRSCAN:
                case TMENDRSCAN:
                case TMENDRSCAN + TMSTARTRSCAN:
                case TMNOFLAGS: {
                    res = recoverGlobalXtns();
                    if (!fromInitialize) {
                        List<Xid> r1 = new ArrayList<Xid>();
                        List<Xid> r2 = new ArrayList<Xid>();
                        for (Xid x : res_cons)
                            r1.add(x);
                        for (Xid x : res) {
                            if (r1.contains(x)) {
                                _logger.debug("XAResourceImpl:recover() continuation xtn=" + x + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
                                r2.add(x);
                            }
                        }

                        Xid[] r =   r2.toArray(new Xid[r2.size()]);
                        synchronized (_recoverLock) {
                            if (res_cons == _recoverRes.get(_proxy))
                                _recoverRes.put(_proxy, r);
                            else
                                return new Xid[0];
                        }
                    } else
                        return res;
                }
                default:
                    throw new XAException(XAException.XAER_INVAL);
            }
        } catch (RemoteException e) {
            _logger.error("XAResourceImpl:recover() , RemoteException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e);
            throw createsXAException(XAException.XAER_RMFAIL, e);
        } catch (CannotCommitException e) {
            _logger.error("XAResourceImpl:recover() , CannotCommitException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e);
            throw createsXAException(XAException.XAER_RMFAIL, e);
        } finally {
            if (_logger.isDebugEnabled()) {
                _logger.debug("XAResourceImpl:recover() terminated, flag: " + flag + " xtns=" + res.length + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
            }

        }

    }


    private Xid[] recoverGlobalXtns()
            throws RemoteException, CannotCommitException {
        List<IRemoteSpace> remotes = _proxy.getDirectProxy().getProxyRouter().getAllAvailableSpaces();
        HashMap<GSServerTransaction, List<TransactionParticipant>> mems = new HashMap<GSServerTransaction, List<TransactionParticipant>>();

        // Build map from all spaces
        for (IRemoteSpace remote : remotes) {
            TransactionInfo[] infos = null;
            try {
                infos = ((IInternalRemoteJSpaceAdmin) remote).getTransactionsInfo(TransactionInfo.Types.XA, TransactionConstants.PREPARED);
            } catch (Exception ex) {
            } //ignore exceptions

            if (infos == null)
                continue;
            for (TransactionInfo info : infos) {
                List<TransactionParticipant> parts = mems.get(((GSServerTransaction) info.getTrasaction()).getId());
                if (parts == null) {
                    parts = new ArrayList<TransactionParticipant>();
                    mems.put(((GSServerTransaction) info.getTrasaction()), parts);
                }
                parts.add(remote);

            }
        }
        TxnMgrProxy mgr = (TxnMgrProxy) m_txnManger;
        ArrayList<Xid> l = new ArrayList<Xid>();
        //we have a map. call the transaction mgr to reenlist relevant entries
        for (Map.Entry<GSServerTransaction, List<TransactionParticipant>> entry : mems.entrySet()) {
            //call the TM only if all the spaces are prepared
            GSServerTransaction txn = entry.getKey();
            List<TransactionParticipant> parts = entry.getValue();
            if (txn.getMetaData() == null || txn.getMetaData().getTransactionParticipantsCount() == 0 || txn.getMetaData().getTransactionParticipantsCount() == parts.size()) {
                mgr.reenterPreparedExternalXid(txn.getId(), parts);
                l.add((GSXid) (txn.getId()));
            }
        }
        //build the list of xids
        Xid[] res = new Xid[l.size()];
        int pos = 0;
        for (Xid x : l) {
            res[pos++] = x;
        }
        return res;
    }


    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#prepare(javax.transaction.xa.Xid)
     */
    public int prepare(Xid xid) throws XAException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("XAResourceImpl:prepare(), the Xid is: " + xid + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
        }

        xid = createGSXid(xid);
        if (_activeEmptyTransactions.remove(xid) != null) { // if empty than read_only
            return XA_RDONLY;
        }
        try {
            int result = m_txnManger.prepare(xid);
            switch (result) { // Try to prepare
                case TransactionParticipant.PREPARED:
                    return XA_OK;
                case TransactionParticipant.NOTCHANGED:
                    return XA_RDONLY;
                default: // Abort
                    throw new XAException(XAException.XA_RBROLLBACK);
            }
        } catch (UnknownTransactionException e) {
            _logger.error("XAResourceImpl:prepare() , UnknownTransactionException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
            throw createsXAException(XAException.XA_RBROLLBACK, e);
        } catch (CannotCommitException e) {
            _logger.error("XAResourceImpl:prepare() , CannotCommitException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
            throw createsXAException(XAException.XA_RBROLLBACK, e);
        } catch (RemoteException e) {
            _logger.error("XAResourceImpl:prepare() , remoteException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
            throw createsXAException(XAException.XA_RBCOMMFAIL, e);
        }
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#forget(javax.transaction.xa.Xid)
     */
    public void forget(Xid xid) throws XAException {
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#rollback(javax.transaction.xa.Xid)
     */
    public void rollback(Xid xid) throws XAException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("XAResourceImpl:rollback(), the Xid is:" + xid + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
        }

        xid = createGSXid(xid);
        try {
            m_txnManger.abort(xid, COMMIT_ABORT_DEFAULT_TIMEOUT);
        } catch (UnknownTransactionException e) {
            if (failOnInvalidRollback)
                _logger.error("XAResourceImpl:rollback() , unknownTxnException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
            throw createsXAException(XAException.XA_RBPROTO, e);
        } catch (CannotAbortException e) {
            _logger.error("XAResourceImpl:rollback() , CannotAbortException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
            throw createsXAException(XAException.XA_RBPROTO, e);
        } catch (RemoteException e) {
            _logger.error("XAResourceImpl:rollback() , remoteException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
            throw createsXAException(XAException.XA_RBCOMMFAIL, e);
        } catch (TimeoutExpiredException e) {
            _logger.error("XAResourceImpl:rollback() , timeoutException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
            throw createsXAException(XAException.XAER_RMFAIL, e);
        } finally {
            _activeEmptyTransactions.remove(xid);
            _suspendedXtns.remove(xid);
            if (_resourcePerSingleTxn)
                _relevantTx = false;
        }
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#end(javax.transaction.xa.Xid, int)
     */
    public void end(Xid xid, int flag) throws XAException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("XAResourceImpl:end(), the Xid is:" + xid + ", flag is:" + flag + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
        }

        xid = createGSXid(xid);
        switch (flag) {
            case TMSUSPEND: {
                Transaction.Created suspended = _proxy.getContextTransaction();
                if (suspended == null) {
                    _logger.error("XAResourceImpl:end() + suspend and current is NULL , Xid = :" + xid + ", flag is:" + flag + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
                    throw new RuntimeException("XAResourceImpl:end() + suspend and current is NULL , Xid = :" + xid + ", flag is:" + flag + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
                }
                _suspendedXtns.put(xid, suspended);
                break;
            }
            case TMSUCCESS:
                break;
            case TMFAIL:
                rollback(xid);
        }
        _proxy.replaceContextTransaction(null);
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#start(javax.transaction.xa.Xid, int)
     */
    public void start(Xid xid, int flag) throws XAException {
        startIn(xid, flag, true);
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#start(javax.transaction.xa.Xid, int)
     */
    protected Transaction.Created startIn(Xid xid, int flag, boolean setAsDefault) throws XAException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("XAResourceImpl:startIn(), the Xid: " + xid + ", the flag: " + flag + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
        }

        xid = createGSXid(xid);

        if (flag != TMRESUME)
            _activeEmptyTransactions.put(xid, xid);
        switch (flag) {
            case TMRESUME: {
                Transaction.Created suspended = _suspendedXtns.remove(xid);
                if (suspended == null) {
                    _logger.error("XAResourceImpl:startIn() + resume and suspended is NULL , Xid = :" + xid + ", flag is:" + flag + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
                    throw new RuntimeException("XAResourceImpl:startIn() + resume and suspended is NULL , Xid = :" + xid + ", flag is:" + flag + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
                }
                if (setAsDefault)
                    _proxy.replaceContextTransaction(suspended, this, _delegatedXa);

                return suspended;
            }
            case TMNOFLAGS:
            case TMJOIN:
                try {
                    return XATransactionFactory.create(m_txnManger, xid, m_timeout * 1000L, setAsDefault, _proxy, this, _delegatedXa);
                } catch (RemoteException e) {
                    _logger.error("XAResourceImpl:startIn() , remoteException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
                    throw createsXAException(XAException.XA_RBCOMMFAIL, e);
                } catch (LeaseDeniedException e) {
                    _logger.error("XAResourceImpl:startIn() , LeaseDeniedException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
                    throw createsXAException(XAException.XAER_INVAL, e);
                }
            default: // TODO check dup ID
                throw new XAException(XAException.XAER_DUPID);
        }
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#commit(javax.transaction.xa.Xid, boolean)
     */
    public void commit(Xid xid, boolean onePhase) throws XAException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("XAResourceImpl:commit(), the xid is:" + xid + ", the onePhase:" + onePhase + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId());
        }

        xid = createGSXid(xid);

        try {
            m_txnManger.commit(xid, COMMIT_ABORT_DEFAULT_TIMEOUT);
        } catch (UnknownTransactionException e) {
            _logger.error("XAResourceImpl:commit() , unknowntxnException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
            throw createsXAException(XAException.XAER_RMERR, e);
        } catch (CannotCommitException e) {
            _logger.error("XAResourceImpl:commit() , cannotcommitException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
            throw createsXAException(XAException.XAER_RMERR, e);
        } catch (RemoteException e) {
            _logger.error("XAResourceImpl:commit() , remoteException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
            throw createsXAException(XAException.XAER_RMFAIL, e);
        } catch (TimeoutExpiredException e) {
            _logger.error("XAResourceImpl:commit() , timeoutexpiredException  rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " exception:" + e + e.getStackTrace());
            throw createsXAException(XAException.XAER_RMFAIL, e);
        } finally {
            _activeEmptyTransactions.remove(xid);
            _suspendedXtns.remove(xid);
            if (_resourcePerSingleTxn)
                _relevantTx = false;
        }
    }

    public void action(Transaction txn) {
        if (txn == null)
            return;
        GSServerTransaction gstxn = (GSServerTransaction) txn;
        Xid res = _activeEmptyTransactions.remove(gstxn.getId());
        if (_logger.isDebugEnabled()) {
            _logger.debug("XAResourceImpl:action(), the xid is:" + gstxn.getId() + " success=" + (res != null) + " rmid=" + _rmid + " thread=" + Thread.currentThread().getId() + " remained=" + _activeEmptyTransactions.size());
        }


    }

    private XAException createsXAException(int errorCode, Exception cause) {
        XAException xe = new XAException(errorCode);
        xe.initCause(cause);
        return xe;
    }

    protected Xid createGSXid(Xid xid) {
        return new GSXid(xid, _rmid);
    }
}
