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

import com.gigaspaces.logger.LogUtils;
import com.sun.jini.thread.RetryTask;
import com.sun.jini.thread.TaskManager;
import com.sun.jini.thread.WakeupManager;

import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.server.ExtendedTransactionManager;
import net.jini.core.transaction.server.TransactionConstants;
import net.jini.core.transaction.server.TransactionManager;

import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A <code>SettlerTask</code> is scheduled task, which causes an unsettled transaction to settle.
 *
 * @author Sun Microsystems, Inc.
 */

@com.gigaspaces.api.InternalApi
public class SettlerTask extends RetryTask implements TransactionConstants {
    private Object tid;
    private int attempt;
    private int maxtries = Integer.MAX_VALUE;
    private ExtendedTransactionManager txnmgr;

    /**
     * Logger for operations related messages
     */
    private static final Logger operationsLogger =
            TxnManagerImpl.operationsLogger;

    /**
     * Logger for transactions related messages
     */
    private static final Logger transactionsLogger =
            TxnManagerImpl.transactionsLogger;

    /**
     * Constructs a <code>SettlerTask</code>.
     *
     * @param manager <code>TaskManager</code> providing the threads of execution.
     * @param txnmgr  <code>TransactionManager</code> which owns the the transaction.
     * @param tid     transaction ID
     */
    public SettlerTask(TaskManager manager, WakeupManager wm,
                       TransactionManager txnmgr, Object tid) {
        super(manager, wm);

        if (txnmgr == null)
            throw new IllegalArgumentException("SettlerTask: SettlerTask: " +
                    "txnmgr must be non-null");
        this.txnmgr = (ExtendedTransactionManager) txnmgr;
        this.tid = tid;
    }

    /**
     * Inherit doc comment from supertype.
     *
     * @see com.sun.jini.thread.RetryTask
     */
    public boolean runAfter(List list, int max) {
        return false;
    }

    public boolean tryOnce() {
        if (operationsLogger.isDebugEnabled()) {
            LogUtils.entering(operationsLogger, SettlerTask.class, "tryOnce");
        }
        try {
            if (attempt >= maxtries)
                return true;

            attempt++;

            if (transactionsLogger.isTraceEnabled()) {
                transactionsLogger.trace(
                        "Attempting to settle transaction id: {0}",
                        tid);
            }

            int state = txnmgr.getState(tid);
            switch (state) {
                case VOTING:
                case COMMITTED:
                    txnmgr.commit(tid, Long.MAX_VALUE);
                    break;

                case ABORTED:
                    txnmgr.abort(tid, Long.MAX_VALUE);
                    break;

                default:
                    if (transactionsLogger.isWarnEnabled()) {
                        transactionsLogger.warn(
                                "Attempting to settle transaction in an invalid state: {0}",
                                new Integer(state));
                    }
            }

        } catch (NoSuchObjectException nsoe) {
            if (transactionsLogger.isWarnEnabled()) {
                transactionsLogger.warn(
                        "Unable to settle recovered transaction", nsoe);
            }
//TODO -ignore?	    
        } catch (TransactionException te) {
            if (transactionsLogger.isDebugEnabled()) {
                transactionsLogger.debug(
                        "Unable to settle recovered transaction", te);
            }
//TODO -ignore?	    
        } catch (RemoteException re) {
            //try again
            if (operationsLogger.isDebugEnabled()) {
                LogUtils.exiting(operationsLogger, SettlerTask.class,"tryOnce", Boolean.valueOf(false));
            }
            return false;
        }

        if (transactionsLogger.isTraceEnabled()) {
            transactionsLogger.trace(
                    "Transaction id {0} was settled",
                    tid);
        }

        if (operationsLogger.isDebugEnabled()) {
            LogUtils.exiting(operationsLogger, SettlerTask.class, "tryOnce", Boolean.valueOf(true));
        }

        return true;
    }
}
