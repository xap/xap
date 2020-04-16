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

package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.internal.cluster.node.impl.replica.data.NotifyTemplateReplicaData;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.cache.TemplateCacheInfo;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.kernel.locks.ILockObject;

import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class NotifyTemplateReplicaProducer
        implements ISingleStageReplicaDataProducer<NotifyTemplateReplicaData> {
    protected final static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_REPLICA);

    private final SpaceEngine _engine;
    protected final Object _requestContext;
    // list of templates uids
    private final Enumeration<String> _allTemplatesSA;

    private int _generatedDataCount;
    private boolean _isClosed;

    public NotifyTemplateReplicaProducer(SpaceEngine engine,
                                         Object requestContext) {
        _engine = engine;
        _requestContext = requestContext;
        _allTemplatesSA = _engine.getCacheManager().getTemplatesManager().getAllTemplatesKeys();
    }

    public synchronized NotifyTemplateReplicaData produceNextData(ISynchronizationCallback syncCallback) {
        if (_isClosed)
            return null;

        while (_allTemplatesSA.hasMoreElements()) {
            TemplateCacheInfo pt = _engine.getCacheManager().getTemplatesManager()
                    .get(_allTemplatesSA.nextElement());

            if (!isRelevant(pt))
                continue;

            NotifyTemplateReplicaData replicaData = buildSpaceReplicaData((NotifyTemplateHolder) pt.m_TemplateHolder, syncCallback);
            if (replicaData == null)
                continue;

            _generatedDataCount++;
            return replicaData;
        }
        close(false);
        return null;
    }

    public CloseStatus close(boolean forced) {
        _isClosed = true;
        return CloseStatus.CLOSED;
    }

    /**
     * @return The constructed recovery entry. Assume IEntryHolder under lock.
     */
    private NotifyTemplateReplicaData buildSpaceReplicaData(
            NotifyTemplateHolder notifyTemplate, ISynchronizationCallback syncCallback) {
        long exp = notifyTemplate.getEntryData().getExpirationTime();
        long ttl = getTimeTolive(exp);
        if (ttl <= 0)
            return null;


        ILockObject entryLock = _engine.getCacheManager().getLockManager().getLockObject(notifyTemplate);
        try {
            synchronized (entryLock) {
                ITemplatePacket templatePacket = buildNotifyTemplatePacket(notifyTemplate, ttl);
                NotifyTemplateReplicaData notifyTemplateReplicaData = new NotifyTemplateReplicaData(templatePacket,
                        notifyTemplate.getUID(),
                        notifyTemplate.getNotifyInfo(),
                        notifyTemplate.getSpaceItemType());

                boolean duplicateUid = syncCallback.synchronizationDataGenerated(notifyTemplateReplicaData);

                if (duplicateUid)
                    return null;

                return notifyTemplateReplicaData;
            }
        } finally {
            _engine.getCacheManager().getLockManager().freeLockObject(entryLock);
        }

    }

    private boolean isRelevant(TemplateCacheInfo pt) {
        if (pt == null
                || pt.m_TemplateHolder == null
                || !pt.m_TemplateHolder.isNotifyTemplate())
            return false;

        NotifyTemplateHolder notifyTemplate = (NotifyTemplateHolder) pt.m_TemplateHolder;
        return isRelevant(notifyTemplate);
    }

    protected boolean isRelevant(NotifyTemplateHolder notifyTemplate) {
        if (!notifyTemplate.isReplicateNotify()) {
            if (_logger.isTraceEnabled())
                _logger.trace(getLogPrefix() + "filtered notify template "
                        + notifyTemplate.getClassName() + " Uid="
                        + notifyTemplate.getUID()
                        + " since it is set not to be replicated");
            return false;
        }

        if (notifyTemplate.getXidOriginatedTransaction() != null) {
            if (_logger.isTraceEnabled())
                _logger.trace(getLogPrefix() + "filtered notify template "
                        + notifyTemplate.getClassName() + " Uid="
                        + notifyTemplate.getUID()
                        + " since it is under transaction");
            return false;
        }

        return true;
    }

    /**
     * @param ttl
     */
    protected ITemplatePacket buildNotifyTemplatePacket(NotifyTemplateHolder templateHolder, long ttl) {
        ITemplatePacket templatePacket = TemplatePacketFactory.createFullPacketForReplication(templateHolder, null);
        templatePacket.setTTL(ttl);

        // TODO OPT: why serialize the type descriptor each time
        templatePacket.setSerializeTypeDesc(true);
        return templatePacket;
    }

    /**
     * @return timeToLeave of IEntryHolder
     */
    private long getTimeTolive(long ttl) {
        return ttl == Long.MAX_VALUE ? ttl : ttl - SystemTime.timeMillis();
    }

    public IReplicationFilterEntry toFilterEntry(NotifyTemplateReplicaData data) {
        return data.toFilterEntry(_engine.getTypeManager());
    }

    protected String getLogPrefix() {
        return _engine.getReplicationNode() + "context [" + _requestContext
                + "] ";
    }

    public String dumpState() {
        return getDumpName() + " producer: completed [" + _isClosed + "] generated data count [" + _generatedDataCount + "]";
    }

    protected String getDumpName() {
        return "Notify template replica";
    }

    @Override
    public String getName() {
        return "NotifyTemplateReplicaProducer";
    }

}
