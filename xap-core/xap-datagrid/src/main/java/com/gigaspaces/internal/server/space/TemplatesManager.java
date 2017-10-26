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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.internal.utils.collections.ConcurrentHashSet;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.TemplateCacheInfo;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Niv Ingberg
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class TemplatesManager {
    private final CacheManager _cacheManager;
    private final ConcurrentHashMap<String, TemplateCacheInfo> _allTemplates;

    private final AtomicInteger _numOfNonNotifyTemplates = new AtomicInteger(0);

    private final Object _notifyTemplatesLock = new Object();
    private int _numOfNotifyWriteTemplates;
    private int _numOfNotifyUpdateTemplates;
    private int _numOfNotifyUnmatchedTemplates;
    private int _numOfNotifyMatchedTemplates;
    private int _numOfNotifyRematchedTemplates;
    private int _numOfNotifyTakeTemplates;
    private int _numOfNotifyLeaseTemplates;
    private volatile boolean _anyNotifyWriteTemplates;
    private volatile boolean _anyNotifyUpdateTemplates;
    private volatile boolean _anyNotifyUnmatchedTemplates;
    private volatile boolean _anyNotifyMatchedTemplates;
    private volatile boolean _anyNotifyRematchedTemplates;
    private volatile boolean _anyNotifyTakeTemplates;
    private volatile boolean _anyNotifyLeaseTemplates;

    //durable notifications
    //note-currently only take nofify are monitored- used in blob store
    private int _numOfDurableNotifyTakeTemplates;
    private volatile boolean _anyDurableNotifyTakeTemplates;


    private final int _numNotifyFifoThreads;
    private final int _numNonNotifyFifoThreads;
    private int _currNotifyFifoThreads;
    private int _currNonNotifyFifoThreads;

    private final FifoTemplatesData _notifyFifoWriteTemplatesData;
    private final FifoTemplatesData _notifyFifoTakeTemplatesData;
    private final FifoTemplatesData _notifyFifoUpdateTemplatesData;
    private final FifoTemplatesData _notifyFifoLeaseExpirationTemplatesData;

    private volatile boolean _anyFifoTemplates;
    private volatile boolean _anyNotifyFifoTemplatesForNonFifoType;
    private volatile boolean _anyNonNotifyFifoTemplates;
    private int _numOfFifoTemplates;
    private int _numNonNotifyFifoTemplates;
    private final boolean[] _anyNonNotifyFifoTemplatesTP;
    private final int[] _numNonNotifyFifoTemplatesTP;

    private long _templateBatchOrder;

    private final ConcurrentHashSet<String> _unallowedClearTakeOptimizationsBlobStoreNotify;

    public TemplatesManager(CacheManager cacheManager, int numNotifyFifoThreads, int numNonNotifyFifoThreads) {
        _cacheManager = cacheManager;
        this._allTemplates = new ConcurrentHashMap<String, TemplateCacheInfo>();

        _numNotifyFifoThreads = numNotifyFifoThreads;
        _numNonNotifyFifoThreads = numNonNotifyFifoThreads;
        _currNotifyFifoThreads = -1;
        _currNonNotifyFifoThreads = -1;
        _notifyFifoWriteTemplatesData = new FifoTemplatesData(numNotifyFifoThreads);
        _notifyFifoTakeTemplatesData = new FifoTemplatesData(numNotifyFifoThreads);
        _notifyFifoUpdateTemplatesData = new FifoTemplatesData(numNotifyFifoThreads);
        _notifyFifoLeaseExpirationTemplatesData = new FifoTemplatesData(numNotifyFifoThreads);
        _anyNonNotifyFifoTemplatesTP = new boolean[numNonNotifyFifoThreads];
        _numNonNotifyFifoTemplatesTP = new int[numNonNotifyFifoThreads];
        _unallowedClearTakeOptimizationsBlobStoreNotify = new ConcurrentHashSet<String>();
    }

    public boolean isEmpty() {
        return _allTemplates.isEmpty();
    }

    /**
     * return true if any notify templates MAY BE stored- no lock on APIs.
     */
    public boolean anyNotifyWriteTemplates() {
        return _anyNotifyWriteTemplates;
    }

    public boolean anyNotifyUpdateTemplates() {
        return _anyNotifyUpdateTemplates;
    }

    public boolean anyNotifyUnmatchedTemplates() {
        return _anyNotifyUnmatchedTemplates;
    }

    public boolean anyNotifyMatchedTemplates() {
        return _anyNotifyMatchedTemplates;
    }

    public boolean anyNotifyRematchedTemplates() {
        return _anyNotifyRematchedTemplates;
    }

    public boolean anyNotifyTakeTemplates() {
        return _anyNotifyTakeTemplates;
    }

    public boolean anyDurableNotifyTakeTemplates() {
        return _anyDurableNotifyTakeTemplates;
    }

    public boolean anyNotifyLeaseTemplates() {
        return _anyNotifyLeaseTemplates;
    }

    public boolean anyNonNotifyTemplates() {
        return _numOfNonNotifyTemplates.get() > 0;
    }

    public boolean anyFifoTemplates() {
        return _anyFifoTemplates;
    }

    public boolean anyNotifyFifoTemplatesForNonFifoType() {
        return _anyNotifyFifoTemplatesForNonFifoType;
    }

    public boolean anyNotifyFifoWriteTemplates() {
        return _notifyFifoWriteTemplatesData.anyNotifyFifoTemplates;
    }

    public boolean anyNotifyFifoWriteTemplates(int tpartition) {
        return _notifyFifoWriteTemplatesData.anyNotifyFifoTemplatesTP[tpartition];
    }

    public boolean anyNotifyFifoWriteTemplatesForNonFifoType() {
        return _notifyFifoWriteTemplatesData.anyNotifyFifoTemplatesForNonFifoType;
    }

    public boolean anyNotifyFifoUpdateTemplates() {
        return _notifyFifoUpdateTemplatesData.anyNotifyFifoTemplates;
    }

    public boolean anyNotifyFifoUpdateTemplates(int tpartition) {
        return _notifyFifoUpdateTemplatesData.anyNotifyFifoTemplatesTP[tpartition];
    }

    public boolean anyNotifyFifoUpdateTemplatesForNonFifoType() {
        return _notifyFifoUpdateTemplatesData.anyNotifyFifoTemplatesForNonFifoType;
    }

    public boolean anyNotifyFifoTakeTemplates() {
        return _notifyFifoTakeTemplatesData.anyNotifyFifoTemplates;
    }

    public boolean anyNotifyFifoTakeTemplates(int tpartition) {
        return _notifyFifoTakeTemplatesData.anyNotifyFifoTemplatesTP[tpartition];
    }

    public boolean anyNotifyFifoTakeTemplatesForNonFifoType() {
        return _notifyFifoTakeTemplatesData.anyNotifyFifoTemplatesForNonFifoType;
    }

    public boolean anyNotifyFifoLeaseExpirationTemplates() {
        return _notifyFifoLeaseExpirationTemplatesData.anyNotifyFifoTemplates;
    }

    public boolean anyNotifyFifoLeaseExpirationTemplates(int tpartition) {
        return _notifyFifoLeaseExpirationTemplatesData.anyNotifyFifoTemplatesTP[tpartition];
    }

    public boolean anyNotifyFifoLeaseExpirationTemplatesForNonFifoType() {
        return _notifyFifoLeaseExpirationTemplatesData.anyNotifyFifoTemplatesForNonFifoType;
    }

    public boolean anyNonNotifyFifoTemplates() {
        return _anyNonNotifyFifoTemplates;
    }

    public boolean anyNonNotifyFifoTemplates(int threadNumber) {
        return _anyNonNotifyFifoTemplatesTP[threadNumber];
    }

    public boolean anyNotifyFifoForNonFifoTypePerOperation(IServerTypeDesc entryTypeDesc, int operation) {
        HashMap<Integer, Integer> notifyFifoTemplatesPerNonFifoType = null;
        if (operation == SpaceOperations.WRITE)
            notifyFifoTemplatesPerNonFifoType = _notifyFifoWriteTemplatesData.notifyFifoTemplatesPerNonFifoType;
        else if (operation == SpaceOperations.UPDATE)
            notifyFifoTemplatesPerNonFifoType = _notifyFifoUpdateTemplatesData.notifyFifoTemplatesPerNonFifoType;
        else if (operation == SpaceOperations.TAKE || operation == SpaceOperations.TAKE_IE)
            notifyFifoTemplatesPerNonFifoType = _notifyFifoTakeTemplatesData.notifyFifoTemplatesPerNonFifoType;
        else if (operation == SpaceOperations.LEASE_EXPIRATION)
            notifyFifoTemplatesPerNonFifoType = _notifyFifoLeaseExpirationTemplatesData.notifyFifoTemplatesPerNonFifoType;

        if (notifyFifoTemplatesPerNonFifoType != null && !notifyFifoTemplatesPerNonFifoType.isEmpty()) {
            for (IServerTypeDesc typeDesc : entryTypeDesc.getSuperTypes()) {
                if (notifyFifoTemplatesPerNonFifoType.containsKey(typeDesc.getTypeName() != null ? typeDesc.getTypeName().hashCode() : 0))
                    return true;
            }
        }
        return false;
    }


    public Enumeration<String> getAllTemplatesKeys() {
        return _allTemplates.keys();
    }

    public TemplateCacheInfo get(String uid) {
        return _allTemplates.get(uid);
    }

    public TemplateCacheInfo put(TemplateCacheInfo template) {
        TemplateCacheInfo prevTemplate = _allTemplates.put(template.m_TemplateHolder.getUID(), template);
        if (prevTemplate == null)
            register(template.m_TemplateHolder);
        return prevTemplate;
    }

    public TemplateCacheInfo putIfAbsent(TemplateCacheInfo template) {
        TemplateCacheInfo prevTemplate = _allTemplates.putIfAbsent(template.m_TemplateHolder.getUID(), template);
        if (prevTemplate == null)
            register(template.m_TemplateHolder);
        return prevTemplate;
    }

    public TemplateCacheInfo remove(String uid) {
        TemplateCacheInfo pTemplate = _allTemplates.remove(uid);
        if (pTemplate != null)
            unregister(pTemplate.m_TemplateHolder);
        return pTemplate;
    }

    public ITemplateHolder getTemplate(String uid) {
        TemplateCacheInfo pTemplate = _allTemplates.get(uid);
        return pTemplate == null ? null : pTemplate.m_TemplateHolder;
    }

    private void register(ITemplateHolder template) {
        if (template.isNotifyTemplate())
            registerNotifyTemplate((NotifyTemplateHolder) template);
        else
            _numOfNonNotifyTemplates.incrementAndGet();

        if (template.isFifoTemplate())
            registerFifoTemplate(template);
    }

    private void unregister(ITemplateHolder template) {
        if (template.isNotifyTemplate())
            unregisterNotifyTemplate((NotifyTemplateHolder) template);
        else
            _numOfNonNotifyTemplates.decrementAndGet();

        if (template.isFifoTemplate())
            unregisterFifoTemplate(template);
    }

    private void registerNotifyTemplate(NotifyTemplateHolder template) {
        boolean ntype = false;
        synchronized (_notifyTemplatesLock) {
            if (template.isBatching())
                template.setBatchOrder(_templateBatchOrder++);

            if (template.containsNotifyType(NotifyActionType.NOTIFY_UPDATE)) {
                _anyNotifyUpdateTemplates = true;
                _numOfNotifyUpdateTemplates++;
                ntype = true;
            }

            if (template.containsNotifyType(NotifyActionType.NOTIFY_UNMATCHED)) {
                _anyNotifyUnmatchedTemplates = true;
                _numOfNotifyUnmatchedTemplates++;
                ntype = true;
            }
            if (template.containsNotifyType(NotifyActionType.NOTIFY_MATCHED_UPDATE)) {
                _anyNotifyMatchedTemplates = true;
                _numOfNotifyMatchedTemplates++;
                ntype = true;
            }
            if (template.containsNotifyType(NotifyActionType.NOTIFY_REMATCHED_UPDATE)) {
                _anyNotifyRematchedTemplates = true;
                _numOfNotifyRematchedTemplates++;
                ntype = true;
            }

            if (template.containsNotifyType(NotifyActionType.NOTIFY_TAKE)) {
                _anyNotifyTakeTemplates = true;
                _numOfNotifyTakeTemplates++;
                ntype = true;
            }

            if (template.containsNotifyType(NotifyActionType.NOTIFY_LEASE_EXPIRATION)) {
                _anyNotifyLeaseTemplates = true;
                _numOfNotifyLeaseTemplates++;
                ntype = true;
            }

            if (template.containsNotifyType(NotifyActionType.NOTIFY_WRITE) || !ntype) {
                _anyNotifyWriteTemplates = true;
                _numOfNotifyWriteTemplates++;
            }
            handleBlobStoreNotifyOptimizations(template, true /*register*/);
        }
    }

    public void registerDurableNotifyTemplate(NotifyTemplateHolder template) {
        synchronized (_notifyTemplatesLock) {
            if (template.containsNotifyType(NotifyActionType.NOTIFY_TAKE)) {
                _anyDurableNotifyTakeTemplates = true;
                _numOfDurableNotifyTakeTemplates++;
            }
            handleBlobStoreNotifyOptimizations(template, true /*register*/);
        }
    }

    public void unregisterDurableNotifyTemplate(NotifyTemplateHolder template) {
        synchronized (_notifyTemplatesLock) {
            if (template.containsNotifyType(NotifyActionType.NOTIFY_TAKE)) {
                _numOfDurableNotifyTakeTemplates--;
                _anyDurableNotifyTakeTemplates = _numOfDurableNotifyTakeTemplates > 0;
            }
            handleBlobStoreNotifyOptimizations(template, false /*register*/);
        }
    }

    private void unregisterNotifyTemplate(NotifyTemplateHolder template) {
        boolean ntype = false;
        synchronized (_notifyTemplatesLock) {
            if (template.containsNotifyType(NotifyActionType.NOTIFY_UPDATE)) {
                _numOfNotifyUpdateTemplates--;
                _anyNotifyUpdateTemplates = _numOfNotifyUpdateTemplates > 0;
                ntype = true;
            }
            if (template.containsNotifyType(NotifyActionType.NOTIFY_UNMATCHED)) {
                _numOfNotifyUnmatchedTemplates--;
                _anyNotifyUnmatchedTemplates = _numOfNotifyUnmatchedTemplates > 0;
                ntype = true;
            }
            if (template.containsNotifyType(NotifyActionType.NOTIFY_MATCHED_UPDATE)) {
                _numOfNotifyMatchedTemplates--;
                _anyNotifyMatchedTemplates = _numOfNotifyMatchedTemplates > 0;
                ntype = true;
            }
            if (template.containsNotifyType(NotifyActionType.NOTIFY_REMATCHED_UPDATE)) {
                _numOfNotifyRematchedTemplates--;
                _anyNotifyRematchedTemplates = _numOfNotifyRematchedTemplates > 0;
                ntype = true;
            }

            if (template.containsNotifyType(NotifyActionType.NOTIFY_TAKE)) {
                _numOfNotifyTakeTemplates--;
                _anyNotifyTakeTemplates = _numOfNotifyTakeTemplates > 0;
                ntype = true;
            }
            if (template.containsNotifyType(NotifyActionType.NOTIFY_LEASE_EXPIRATION)) {
                _numOfNotifyLeaseTemplates--;
                _anyNotifyLeaseTemplates = _numOfNotifyLeaseTemplates > 0;
                ntype = true;
            }
            if (template.containsNotifyType(NotifyActionType.NOTIFY_WRITE) || !ntype) {
                _numOfNotifyWriteTemplates--;
                _anyNotifyWriteTemplates = _numOfNotifyWriteTemplates > 0;
            }
            handleBlobStoreNotifyOptimizations(template, false /*register*/);
        }
    }


    private synchronized void registerFifoTemplate(ITemplateHolder template) {
        int maxThreads = template.isNotifyTemplate() ? _numNotifyFifoThreads : _numNonNotifyFifoThreads;
        if (maxThreads != 1) {
            synchronized (this) {
                if (template.isNotifyTemplate()) {
                    _currNotifyFifoThreads = (_currNotifyFifoThreads + 1) % maxThreads;
                    template.setFifoThreadPartition(_currNotifyFifoThreads);
                } else {
                    _currNonNotifyFifoThreads = (_currNonNotifyFifoThreads + 1) % maxThreads;
                    template.setFifoThreadPartition(_currNonNotifyFifoThreads);
                }
            }
        }

        _numOfFifoTemplates++;
        _anyFifoTemplates = true;

        int tpartition = template.getFifoThreadPartition();
        if (template.isNotifyTemplate()) {
            NotifyTemplateHolder notifyTemplate = (NotifyTemplateHolder) template;
            boolean notifyfifoClass = template.getServerTypeDesc().isFifoSupported();
            boolean ntype = false;
            if (notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_UPDATE) || notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_UNMATCHED)
                    || notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_MATCHED_UPDATE) || notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_REMATCHED_UPDATE)) {
                incrementNumFifoTemplatesData(_notifyFifoUpdateTemplatesData, template, notifyfifoClass, tpartition);
                ntype = true;
            }
            if (notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_TAKE)) {
                incrementNumFifoTemplatesData(_notifyFifoTakeTemplatesData, template, notifyfifoClass, tpartition);
                ntype = true;
            }
            if (notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_LEASE_EXPIRATION)) {
                incrementNumFifoTemplatesData(_notifyFifoLeaseExpirationTemplatesData, template, notifyfifoClass, tpartition);
                ntype = true;
            }
            if (notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_WRITE) || !ntype) {
                incrementNumFifoTemplatesData(_notifyFifoWriteTemplatesData, template, notifyfifoClass, tpartition);
            }
        } else {
            _numNonNotifyFifoTemplates++;
            _anyNonNotifyFifoTemplatesTP[tpartition] = true;
            _numNonNotifyFifoTemplatesTP[tpartition]++;
            _anyNonNotifyFifoTemplates = true;
        }
    }

    private void incrementNumFifoTemplatesData(FifoTemplatesData fifoTemplatesData, ITemplateHolder templateHolder,
                                               boolean notifyfifoClass, int tpartition) {
        fifoTemplatesData.numNotifyFifoTemplates++;
        fifoTemplatesData.anyNotifyFifoTemplatesTP[tpartition] = true;
        fifoTemplatesData.numNotifyFifoTemplatesTP[tpartition]++;
        if (!notifyfifoClass) {
            fifoTemplatesData.numNotifyFifoTemplatesForNonFifoType++;
            HashMap<Integer, Integer> notifyFifoTemplatesPerNonFifoType = fifoTemplatesData.notifyFifoTemplatesPerNonFifoType == null ?
                    new HashMap<Integer, Integer>() : new HashMap<Integer, Integer>(fifoTemplatesData.notifyFifoTemplatesPerNonFifoType);

            Integer cur = notifyFifoTemplatesPerNonFifoType.get(templateHolder.getClassName() != null ? templateHolder.getClassName().hashCode() : 0);
            if (cur != null)
                notifyFifoTemplatesPerNonFifoType.put(templateHolder.getClassName().hashCode(), cur.intValue() + 1);
            else
                notifyFifoTemplatesPerNonFifoType.put(templateHolder.getClassName().hashCode(), 1);
            fifoTemplatesData.notifyFifoTemplatesPerNonFifoType = notifyFifoTemplatesPerNonFifoType;

            _anyNotifyFifoTemplatesForNonFifoType = true;
            fifoTemplatesData.anyNotifyFifoTemplatesForNonFifoType = true;
        }
        fifoTemplatesData.anyNotifyFifoTemplates = true;
    }

    private synchronized void unregisterFifoTemplate(ITemplateHolder template) {
        _numOfFifoTemplates--;
        _anyFifoTemplates = _numOfFifoTemplates > 0;
        int tpartition = template.getFifoThreadPartition();
        if (template.isNotifyTemplate()) {
            NotifyTemplateHolder notifyTemplate = (NotifyTemplateHolder) template;
            boolean notifyfifoClass = template.getServerTypeDesc().isFifoSupported();
            boolean ntype = false;
            if (notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_UPDATE) || notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_UNMATCHED)
                    || notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_MATCHED_UPDATE) || notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_REMATCHED_UPDATE)) {
                decrementNumFifoTemplatesData(_notifyFifoUpdateTemplatesData, template, notifyfifoClass, tpartition);
                ntype = true;
            }
            if (notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_TAKE)) {
                decrementNumFifoTemplatesData(_notifyFifoTakeTemplatesData, template, notifyfifoClass, tpartition);
                ntype = true;
            }
            if (notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_LEASE_EXPIRATION)) {
                decrementNumFifoTemplatesData(_notifyFifoLeaseExpirationTemplatesData, template, notifyfifoClass, tpartition);
                ntype = true;
            }
            if (notifyTemplate.containsNotifyType(NotifyActionType.NOTIFY_WRITE) || !ntype) {
                decrementNumFifoTemplatesData(_notifyFifoWriteTemplatesData, template, notifyfifoClass, tpartition);
            }
        } else {
            _numNonNotifyFifoTemplates--;
            _numNonNotifyFifoTemplatesTP[tpartition]--;
            _anyNonNotifyFifoTemplatesTP[tpartition] = _numNonNotifyFifoTemplatesTP[tpartition] > 0;
            _anyNonNotifyFifoTemplates = _numNonNotifyFifoTemplates > 0;
        }
    }

    private void decrementNumFifoTemplatesData(FifoTemplatesData fifoTemplatesData, ITemplateHolder templateHolder,
                                               boolean notifyfifoClass, int tpartition) {
        fifoTemplatesData.numNotifyFifoTemplates--;
        fifoTemplatesData.numNotifyFifoTemplatesTP[tpartition]--;
        fifoTemplatesData.anyNotifyFifoTemplatesTP[tpartition] = fifoTemplatesData.numNotifyFifoTemplatesTP[tpartition] > 0;
        if (!notifyfifoClass) {
            fifoTemplatesData.numNotifyFifoTemplatesForNonFifoType--;
            HashMap<Integer, Integer> notifyFifoTemplatesPerNonFifoType = fifoTemplatesData.notifyFifoTemplatesPerNonFifoType == null ?
                    new HashMap<Integer, Integer>() : new HashMap<Integer, Integer>(fifoTemplatesData.notifyFifoTemplatesPerNonFifoType);

            Integer cur = notifyFifoTemplatesPerNonFifoType.get(templateHolder.getClassName() != null ? templateHolder.getClassName().hashCode() : 0);
            if (cur == 1)
                notifyFifoTemplatesPerNonFifoType.remove(templateHolder.getClassName().hashCode());
            else
                notifyFifoTemplatesPerNonFifoType.put(templateHolder.getClassName().hashCode(), cur.intValue() - 1);

            fifoTemplatesData.notifyFifoTemplatesPerNonFifoType = notifyFifoTemplatesPerNonFifoType;
            fifoTemplatesData.anyNotifyFifoTemplatesForNonFifoType = fifoTemplatesData.numNotifyFifoTemplatesForNonFifoType > 0;
            _anyNotifyFifoTemplatesForNonFifoType = _notifyFifoUpdateTemplatesData.anyNotifyFifoTemplatesForNonFifoType
                    || _notifyFifoTakeTemplatesData.anyNotifyFifoTemplatesForNonFifoType || _notifyFifoWriteTemplatesData.anyNotifyFifoTemplatesForNonFifoType || _notifyFifoLeaseExpirationTemplatesData.anyNotifyFifoTemplatesForNonFifoType;
        }
        fifoTemplatesData.anyNotifyFifoTemplates = fifoTemplatesData.numNotifyFifoTemplates > 0;
    }

    private void setBlobStoreClearTakeOptimizationNotAllowed(String typeName) {
        _unallowedClearTakeOptimizationsBlobStoreNotify.add(typeName);
    }

    public boolean isBlobStoreClearTakeOptimizationAllowedNotify(IServerTypeDesc type, boolean onBackup) {
        {
            if (onBackup && !anyDurableNotifyTakeTemplates())
                return true;
            if (_unallowedClearTakeOptimizationsBlobStoreNotify.isEmpty())
                return true;
            if (_unallowedClearTakeOptimizationsBlobStoreNotify.contains(type.getTypeDesc().getTypeName()))
                return false;
            for (String typeName : _unallowedClearTakeOptimizationsBlobStoreNotify)
            {
                for (IServerTypeDesc typeDesc : type.getSuperTypes())
                    if (typeDesc.getTypeName().equals(typeName))
                        return false;
            }

            return true;
        }
    }

    private void handleBlobStoreNotifyOptimizations(NotifyTemplateHolder template, boolean register) {
        if (!_cacheManager.isBlobStoreCachePolicy() || !(template.containsNotifyType(NotifyActionType.NOTIFY_TAKE)))
            return;
        if (!register) {
            if (!anyDurableNotifyTakeTemplates() && !anyNotifyTakeTemplates())
                _unallowedClearTakeOptimizationsBlobStoreNotify.clear();
        } else
            setBlobStoreClearTakeOptimizationNotAllowed(template.getClassName());
    }

    /**
     * Holds all the counters for specific type of notify templates
     */
    public static class FifoTemplatesData {
        public volatile boolean anyNotifyFifoTemplates;
        public volatile boolean anyNotifyFifoTemplatesForNonFifoType;
        public volatile HashMap<Integer, Integer> notifyFifoTemplatesPerNonFifoType;
        public int numNotifyFifoTemplates;
        public int numNotifyFifoTemplatesForNonFifoType;
        public final boolean[] anyNotifyFifoTemplatesTP;
        public final int[] numNotifyFifoTemplatesTP;

        public FifoTemplatesData(int notifyFifoThreads) {
            anyNotifyFifoTemplatesTP = new boolean[notifyFifoThreads];
            numNotifyFifoTemplatesTP = new int[notifyFifoThreads];
        }
    }
}
