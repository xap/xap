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


package com.j_spaces.core.fifo;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cache.TerminatingFifoXtnsInfo;

import net.jini.core.transaction.server.ServerTransaction;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * holds info for scanning waiting templates as a result from fifo operations both notify and
 * non-notify templates
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class FifoBackgroundRequest {
    private static final Logger _loggerFifo = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_FIFO);

    private final OperationID _operID;
    private final boolean _isNotifyRequest;   //handle notify templates
    private final boolean _isNonNotifyRequest; //handle non-notify templates
    private final IEntryHolder _entry;  // entry triggered
    private final IEntryHolder _originalEntry;
    private final boolean _fromReplication;
    private final int _spaceOperation; //op that occurred
    private final ServerTransaction _xtn;  //operation under-xtn

    private volatile boolean _notReady;   // in absolute order -we can process this entry if notReady =false
    private volatile boolean _cancelled;   // we can ignore this entry
    private ITemplateHolder _template;  //operate the entry/entries on a specific fifo template
    private long _time;   //time when inserted
    private IEntryHolder _cloneEH;
    private boolean _xtnEnd; //is this record created as a result of xtn end ?
    //the fifo serial xtn when the operation that caused this event happened
    private long _fifoXtnNumber = TerminatingFifoXtnsInfo.UNKNOWN_FIFO_XTN;
    private AllowFifoNotificationsForNonFifoType _allowFifoNotificationsForNonFifoEvents;

    public FifoBackgroundRequest() {
        this(null, false, false, null, null, false, 0, null, null);
    }

    public FifoBackgroundRequest(OperationID operID, boolean isNotifyRequest,
                                 boolean isNonNotifyRequest, IEntryHolder eh, IEntryHolder originalEntry,
                                 boolean fromReplication, ITemplateHolder template) {
        this(operID, isNotifyRequest, isNonNotifyRequest, eh, originalEntry, fromReplication,
                template.getTemplateOperation(), template.getXidOriginatedTransaction(), null);
        _template = template;
    }

    public FifoBackgroundRequest(OperationID operID, boolean isNotifyRequest,
                                 boolean isNonNotifyRequest, IEntryHolder eh, IEntryHolder originalEntry,
                                 boolean fromReplication, int spaceOperation, ServerTransaction txn, IEntryHolder cloneEH) {
        _operID = operID;
        _isNotifyRequest = isNotifyRequest;
        _isNonNotifyRequest = isNonNotifyRequest;
        _entry = eh;
        _originalEntry = originalEntry;
        _spaceOperation = spaceOperation;
        _xtn = txn;
        _fromReplication = fromReplication;
        _cloneEH = cloneEH;
    }

    public boolean isNotifyRequest() {
        return _isNotifyRequest;
    }

    public boolean isNonNotifyRequest() {
        return _isNonNotifyRequest;
    }

    public long getFifoXtnNumber() {
        return _fifoXtnNumber;
    }

    public void setFifoXtnNumber(long xtnFifo) {
        _fifoXtnNumber = xtnFifo;
    }

    public boolean isReady() {
        return !_notReady;
    }

    public void setReady() {
        _notReady = false;
    }

    public void resetReady() {
        _notReady = true;
    }

    public boolean isCancelled() {
        return _cancelled;
    }

    public void setCancelled() {
        _cancelled = true;
    }

    public IEntryHolder getEntry() {
        return _entry;
    }


    public IEntryHolder getOriginalEntry() {
        return _originalEntry;
    }

    public ITemplateHolder getTemplate() {
        return _template;
    }

    public long getTime() {
        return _time;
    }

    public void setTime(long tm) {
        _time = tm;
    }

    public int getSpaceOperation() {
        return _spaceOperation;
    }

    public IEntryHolder getCloneEntry() {
        return _cloneEH;
    }

    public ServerTransaction getXtn() {
        return _xtn;  //operation under-xtn
    }

    public boolean isXtnEnd() {
        return _xtnEnd; //is this record created as a result of xtn end ?
    }

    public void setXtnEnd() {
        _xtnEnd = true; //is this record created as a result of xtn end ?
    }

    public boolean isFromReplication() {
        return _fromReplication;
    }

    public OperationID getOperationID() {
        return _operID;
    }

    public AllowFifoNotificationsForNonFifoType getAllowFifoNotificationsForNonFifoEvents() {
        return _allowFifoNotificationsForNonFifoEvents;
    }

    public void setAllowFifoNotificationsForNonFifoEvents(AllowFifoNotificationsForNonFifoType e) {
        _allowFifoNotificationsForNonFifoEvents = e;
    }

    public static class AllowFifoNotificationsForNonFifoType {
        //this class is used in order for fifo notifications to
        //wait till the xtn is not active without the need to lock the xtn table-
        //it is used in fifo-notifications for non-fifo events
        //locking the xtn table is done only in fifo notifications for fifo events
        private boolean allowed;

        public void waitTillAllowed(long timeLimit) {
            if (allowed)
                return;
            synchronized (this) {
                if (allowed)
                    return;
                try {
                    this.wait(timeLimit);
                } catch (InterruptedException e) {
                    if (_loggerFifo.isErrorEnabled())
                        _loggerFifo.error(" InterruptedException while waiting for operation to become available for notifications", e);
                    Thread.currentThread().interrupt();
                }
                if (!allowed) {
                    if (_loggerFifo.isWarnEnabled())
                        _loggerFifo.warn(" fifo notifications- operation didn't become available for notifications within designated time- notification allowed");
                    allowed = true; //needless to block others
                }
            }
        }

        public void allow() {
            synchronized (this) {
                allowed = true;
                this.notifyAll();
            }
        }
    }

}
