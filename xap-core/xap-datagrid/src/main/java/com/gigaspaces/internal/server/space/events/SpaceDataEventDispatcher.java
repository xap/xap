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

package com.gigaspaces.internal.server.space.events;

import com.gigaspaces.exception.lrmi.SlowConsumerException;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.LeaseManager;
import com.j_spaces.kernel.IConsumerObject;

import net.jini.core.event.UnknownEventException;
import net.jini.core.lease.UnknownLeaseException;

import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.j_spaces.core.Constants.Engine.NOTIFIER_TIME_LIMIT;

@com.gigaspaces.api.InternalApi
public class SpaceDataEventDispatcher implements IConsumerObject<RemoteEventBusPacket> {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_NOTIFY);

    private final SpaceDataEventManager _dataEventManager;
    private LeaseManager _leaseManager;

    public SpaceDataEventDispatcher(SpaceDataEventManager dataEventManager) {
        this._dataEventManager = dataEventManager;
    }

    public void setLeaseManager(LeaseManager leaseManager) {
        this._leaseManager = leaseManager;
    }

    @Override
    public void cleanUp() {
    }

    /**
     * dispatch in an asynchronous manner a remote event object via a remote event listener ( which
     * is retrieved through the RemoteEventBusPacket ). The dispatch will take place if the remote
     * event listener exists. If the dispatching will fail with the UnknownEventException the
     * template will be removed from the SpaceEngine.
     */
    @Override
    public void dispatch(RemoteEventBusPacket packet) {
        try {
            packet.execute(this);
        } catch (Exception ex) {
            if (_logger.isErrorEnabled())
                _logger.error(ex.toString(), ex);
        } finally {
            _dataEventManager.notifyReturned(packet.getStatus(), (ITemplateHolder) packet.getEntryHolder());
        }
    }

    public void execute(RemoteEventBusPacket re) throws Exception {
        final int ttl = re.getTTL();
        NotifyTemplateHolder th = (NotifyTemplateHolder) re.getEntryHolder();
        boolean signaledRemoteException = false;
        while (true) {
            try {
                // checks if there is a valid remote event listener at all.
                if (th.getREListener() == null || th.isDeleted())
                    return;

                synchronized (th) {
                    if (th.getREListener() == null || th.isDeleted())
                        return;
                }
                if (th.hasPendingRemoteException() && !signaledRemoteException) {
                    re.setTTL(re.getTTL() - 1);
                    if (re.getTTL() > 0) {
                        if (_logger.isDebugEnabled()) {
                            _logger.debug("Notification failed (signaled by other thread) " +
                                    "\nRetrying: TTL=" + re.getTTL());
                        }
                        try {
                            Thread.sleep(NOTIFIER_TIME_LIMIT);
                        } catch (InterruptedException ie) {
                            if (_logger.isTraceEnabled()) {
                                _logger.trace(Thread.currentThread().getName() + " interrupted.", ie);
                            }

                            cancel(th);

                            //Restore the interrupted status
                            Thread.currentThread().interrupt();

                            return;
                        }
                        continue; //retry
                    } else {
                        //I give up
                        if (_logger.isDebugEnabled())
                            _logger.debug("Notification failed: gave up after " + (NOTIFIER_TIME_LIMIT * ttl) + " milliseconds ; removing notification template.");
                        cancel(th);
                        return;
                    }
                }

                _dataEventManager.executePacket(re, th);

                if (th.hasPendingRemoteException()) {
                    synchronized (th) {
                        th.setPendingRemoteException(false);
                    }
                }
                return;
            } catch (UnknownEventException ex) {
                cancel(th, ex);
                return;
            } catch (java.rmi.NoSuchObjectException ex) {
                cancel(th, ex);
                return;
            } catch (RemoteException rex) {
                re.setTTL(re.getTTL() - 1);
                if (re.getTTL() > 0 && !(rex.getCause() instanceof SlowConsumerException)) {
                    synchronized (th) {
                        if (!th.hasPendingRemoteException()) {
                            signaledRemoteException = true;
                            th.setPendingRemoteException(true);
                        }
                    }
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Notification failed.", rex);
                        _logger.debug("Retrying: TTL=" + re.getTTL());
                    }
                    // sleeping for a radom time (up to 10 seconds)
                    try {
                        Thread.sleep(NOTIFIER_TIME_LIMIT);
                    } catch (InterruptedException ie) {
                        if (_logger.isTraceEnabled()) {
                            _logger.trace(Thread.currentThread().getName() + " interrupted.", ie);
                        }

                        cancel(th);
                        //Restore the interrupted status
                        Thread.currentThread().interrupt();

                        return;
                    }
                    continue; //retry
                } else {
                    cancel(th, rex);
                    return;
                }
            } catch (Exception ex) {
                if (_logger.isErrorEnabled()) {
                    _logger.error("Notification failed. ", ex);
                }

                cancel(th, ex);
                return;
            }
        }
    }

    private void cancel(NotifyTemplateHolder template, Exception ex) {
        if (_logger.isDebugEnabled())
            _logger.debug("Notification failed: " + ex + " ; removing notification template.");

        cancel(template);
    }

    private void cancel(NotifyTemplateHolder template) {
        synchronized (template) {
            template.setREListener(null);
        }

        if (!template.isDeleted()) {
            try {
                _leaseManager.cancel(template.getUID(),
                        template.getClassName(), template.getSpaceItemType(),
                        false /*fromReplication*/, true /*origin*/, false /* isFromGateway */);
            } catch (UnknownLeaseException e) {
                if (_logger.isTraceEnabled())
                    _logger.trace(Thread.currentThread().getName() + " fail to cancel lease.", e);
            }
        }
    }
}
