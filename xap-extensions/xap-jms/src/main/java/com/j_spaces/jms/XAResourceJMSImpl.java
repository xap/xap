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

/*
 * Created on 15/03/2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.j_spaces.jms;

import com.gigaspaces.client.transaction.xa.GSServerTransaction;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.client.XAResourceImpl;

import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.server.TransactionManager;

import java.rmi.RemoteException;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * An instance of this class represents a XAResource implementation for JMS
 *
 * @author Guy Korland
 * @version 4.5
 **/
public class XAResourceJMSImpl extends XAResourceImpl {
    private GSXASessionImpl session = null;

    // logger
    private static Logger logger = LoggerFactory.getLogger(Constants.LOGGER_XA);

    /**
     * Map from Xid to transaction <p> <b>Key:</b> <code>XID</code> <br> <b>Object:</b>
     * <code>Transaction</code>
     */
    private HashMap<Xid, XATrasactionContext> transactionsTable;


    public XAResourceJMSImpl(TransactionManager tm,
                             GSXASessionImpl session,
                             ISpaceProxy proxy) {
        super(tm, proxy);
        this.session = session;
        transactionsTable = new HashMap<Xid, XATrasactionContext>();
        if (logger.isDebugEnabled()) {
            logger.debug("Initialized: " + toString());
        }
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#start(javax.transaction.xa.Xid, int)
     */
    @Override
    public void start(Xid xid, int flag) throws XAException {
        Xid originalxid = xid;
        if (logger.isDebugEnabled()) {
            logger.debug("Associating transaction: " + xid);
        }
        xid = createGSXid(xid);
        Transaction.Created cr = startIn(originalxid, flag, false);
        XATrasactionContext context = transactionsTable.get(xid);
        if (context == null) {
            context = new XATrasactionContext(cr.transaction);
            transactionsTable.put(xid, context);
        } else {
            context.setTransaction(cr.transaction);
        }
        context.setStatus(XATrasactionContext.SUCCESS);
        session.setTransaction(context);
    }


    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#end(javax.transaction.xa.Xid, int)
     */
    @Override
    public void end(Xid xid, int flag) throws XAException {
        if (logger.isDebugEnabled()) {
            logger.debug("Disassociating transaction: " + xid);
        }
        session.setTransaction(null);
        xid = createGSXid(xid);
        XATrasactionContext context = transactionsTable.get(xid);
        if (context != null) {
            boolean isEmpty = _activeEmptyTransactions.contains(xid) && context.getSentMessages().isEmpty();
            if (!isEmpty)
                _activeEmptyTransactions.remove(xid);
            switch (flag) {
                case TMSUCCESS:
                    context.setStatus(XATrasactionContext.SUCCESS);
                    break;
                case TMSUSPEND:
                    context.setStatus(XATrasactionContext.SUSPENDED);
                    break;
                case TMFAIL:
                    context.setStatus(XATrasactionContext.ROLLBACK_ONLY);
                    rollback(xid);
                    break;
                default:
                    break;
            }
        }
        //super.end(xid, flag);
        _proxy.replaceContextTransaction(null);
    }


    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#prepare(javax.transaction.xa.Xid)
     */
    @Override
    public int prepare(Xid xid) throws XAException {
        if (logger.isDebugEnabled()) {
            logger.debug("Preparing transaction: " + xid);
        }
        Xid originalxid = xid;
        xid = createGSXid(xid);
        XATrasactionContext context = transactionsTable.get(xid);
        if (context != null) {
            try {
                session.sendMessages(context.getSentMessages(), context.getTransaction());
            } catch (RemoteException e) {
                throw new XAException(XAException.XA_RBCOMMFAIL);
            } catch (Exception e) {
                throw new XAException(XAException.XA_RBROLLBACK);
            }
        }
        int result = super.prepare(originalxid);
        return result;
    }


    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#commit(javax.transaction.xa.Xid, boolean)
     */
    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        if (logger.isDebugEnabled()) {
            logger.debug("Committing transaction: " + xid);
        }

        // TODO: in case of onePhase, do we need to prepare here??
        if (onePhase) {
            prepare(xid);
        }
        Xid originalxid = xid;
        xid = createGSXid(xid);

        super.commit(originalxid, onePhase);
        transactionsTable.remove(xid);
    }


    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#rollback(javax.transaction.xa.Xid)
     */
    @Override
    public void rollback(Xid xid) throws XAException {
        if (logger.isDebugEnabled()) {
            logger.debug("Rolling back transaction: " + xid);
        }
        Xid originalxid = xid;
        xid = createGSXid(xid);

        // this is a patch because prepare calls rollback in our implementation
        // and the transaction manager calls it too. So we make it work once.
        if (transactionsTable.containsKey(xid)) {
            super.rollback(originalxid);
            transactionsTable.remove(xid);
        }
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#forget(javax.transaction.xa.Xid)
     */
    @Override
    public void forget(Xid xid) throws XAException {
        if (logger.isDebugEnabled()) {
            logger.debug("Forgetting transaction: " + xid);
        }
        xid = createGSXid(xid);
        transactionsTable.remove(xid);
    }


    /* (non-Javadoc)
     * @see javax.transaction.xa.XAResource#isSameRM(javax.transaction.xa.XAResource)
     */
    @Override
    public boolean isSameRM(XAResource xares) throws XAException {
        if (logger.isDebugEnabled()) {
            logger.debug("Comparing XAResource to: " + xares);
        }
        if (xares == this) {
            return true;
        }
        try {
            if (super.isSameRM(xares) && xares instanceof XAResourceJMSImpl) {
                XAResourceJMSImpl xaresImpl = (XAResourceJMSImpl) xares;
                return session.equals(xaresImpl.session);
            }
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Exception while comparing XAResource to: " + xares);
            }
        }
        return false;
    }


    @Override
    public String toString() {
        return "XAResourceJMSImpl:\nLocalTransactionManager: " +
                m_txnManger + "\nXASession: " + session.m_sessionID;
    }

    public void action(Transaction txn) {
        if (txn != null && txn.equals(session.getTransaction())) {
            GSServerTransaction gstxn = (GSServerTransaction) txn;
            _activeEmptyTransactions.remove(gstxn.getId());
        }
    }

}
