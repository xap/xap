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

package com.gigaspaces.internal.client.spaceproxy.transaction;

import com.j_spaces.core.client.ActionListener;
import com.j_spaces.core.client.XAResourceImpl;

import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.Transaction.Created;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * @author anna
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyTransactionManager {
    /**
     * If true means that the _contextTransactions should be checked when no explicit transaction is
     * found. Notice: no need to set it as volatile since the same thread that set it to true is the
     * only one that must see it as true.
     */
    private boolean _supportsContextTransaction;
    private final ThreadLocal<CurrentXtnProxyHolder> _contextTransactions;

    /**
     * Current XAResource.
     */
    private transient ActionListener _actionListener;
    final private static Logger _logger =
            LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_XA);

    public SpaceProxyTransactionManager() {
        this._contextTransactions = new ThreadLocal<CurrentXtnProxyHolder>();
    }

    public void setActionListener(ActionListener actionListener) {
        _actionListener = actionListener;
    }

    public Transaction.Created getContextTransaction() {
        CurrentXtnProxyHolder ph = _contextTransactions.get();
        return ph != null ? ph.getXtn() : null;
    }

    public Transaction.Created replaceContextTransaction(Transaction.Created txn) {
        return replaceContextTransaction(txn, null, false);
    }

    public Transaction.Created replaceContextTransaction(Transaction.Created txn, ActionListener currentActionListener, boolean delegatedXa) {
        if (_logger.isDebugEnabled()) {
            if (txn == null) {
                _logger.debug("GS:replaceContextTransaction called with txn NULL  thread=" + Thread.currentThread().getId() + " proxy=" + this);
            } else {
                XAResourceImpl xares = null;
                if (currentActionListener != null)
                    xares = (XAResourceImpl) currentActionListener;
                if (xares != null)
                    _logger.debug("GS:replaceContextTransaction called with txn  thread=" + Thread.currentThread().getId() + " rmid=" + xares.getRmid() + " proxy=" + this);
                else
                    _logger.debug("GS:replaceContextTransaction called with txn  thread=" + Thread.currentThread().getId() + " rmid= = NULL" + " proxy=" + this);
            }
        }

        Transaction.Created oldTxn = getContextTransaction();
        if (txn != null) {
            _supportsContextTransaction = true;
            _contextTransactions.set(new CurrentXtnProxyHolder(txn, currentActionListener, delegatedXa));
        } else
            _contextTransactions.remove();

        return oldTxn;
    }

    public Transaction beforeSpaceAction(Transaction transaction) {
        Transaction res_transaction = transaction;
        boolean consideredActionListener = false;
        ActionListener currentActionListener = null;
        CurrentXtnProxyHolder ph = _supportsContextTransaction ? _contextTransactions.get() : null;

        boolean ignore = res_transaction == null && ph != null && ph.isdelegatedXa();
        if (!ignore) {
            if (res_transaction == null || ((ph != null && ph.getCurrentActionListener() != null && ph.getXtn().transaction.equals(transaction)))) {
                if (ph != null) {
                    consideredActionListener = true;
                    res_transaction = ph.getXtn().transaction;
                    currentActionListener = ph.getCurrentActionListener();
                    if (currentActionListener == null)
                        currentActionListener = _actionListener;
                    if (currentActionListener != null)
                        currentActionListener.action(res_transaction);
                }
            }
        }
        if (!ignore && _actionListener != null && !consideredActionListener)
            _actionListener.action(transaction);
        return res_transaction;
    }


    private static class CurrentXtnProxyHolder {
        private final Created _currentXtn;
        private final ActionListener _currentActionListener;
        private final boolean _delegatedXa;

        CurrentXtnProxyHolder(Created currentXtn, ActionListener currentActionListener, boolean delegatedXa) {
            _currentXtn = currentXtn;
            _currentActionListener = currentActionListener;
            _delegatedXa = delegatedXa;
        }

        Created getXtn() {
            return _currentXtn;
        }

        ActionListener getCurrentActionListener() {
            return _currentActionListener;
        }

        boolean isdelegatedXa() {
            return _delegatedXa;
        }
    }
}
