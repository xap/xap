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
 * Title: LeaseManager.java Description: Manages expired leases for all types
 * Company: 	 GigaSpaces Technologies
 * @author		 Moran Avigdor
 * @version		 1.0 27/11/2005
 * @since		 5.0EAG Build#1314
 */
package com.j_spaces.core;

import com.gigaspaces.internal.backport.java.util.concurrent.FastConcurrentSkipListMap;
import com.gigaspaces.internal.lease.LeaseUtils;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceEngine.TemplateRemoveReasonCodes;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.eviction.RecentDeletesRepository;
import com.gigaspaces.internal.server.space.eviction.RecentUpdatesRepository;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacket;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.cache.CacheManager.RecentDeleteCodes;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.ILeasedEntryCacheInfo;
import com.j_spaces.core.cache.TemplateCacheInfo;
import com.j_spaces.core.cache.TerminatingFifoXtnsInfo.FifoXtnEntryInfo;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.blobStore.IBlobStoreEntryHolder;
import com.j_spaces.core.cluster.ReplicationOperationType;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.core.server.processor.Processor;
import com.j_spaces.core.transaction.Prepared2PCXtnInfo;
import com.j_spaces.core.transaction.TransactionHandler;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.StoredListFactory;
import com.j_spaces.kernel.locks.ILockObject;

import net.jini.core.lease.Lease;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.space.InternalSpaceException;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.j_spaces.core.Constants.LeaseManager.LM_BACKUP_EXPIRATION_DELAY_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_BACKUP_EXPIRATION_DELAY_PROP;
import static com.j_spaces.core.Constants.LeaseManager.LM_CHECK_TIME_MARKERS_REPOSITORY_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_DISABLE_ENTRIES_LEASES_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_DISABLE_ENTRIES_LEASES_PROP;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_FIFOENTRY_XTNINFO;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_INTERVAL_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_INTERVAL_PROP;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_PENDING_ANSWERS_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_DELETES_CHECK_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_DELETES_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_DELETES_PROP;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_UPDATES_CHECK_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_UPDATES_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_UPDATES_PROP;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_STALE_REPLICAS_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_STALE_REPLICAS_PROP;
import static com.j_spaces.core.Constants.LeaseManager.LM_SEGMEENTS_PER_EXPIRATION_CELL_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_SEGMEENTS_PER_EXPIRATION_CELL_PROP;

/**
 * Lease Manager handles operations that can be carried out on a lease: creation, renewal and
 * cancellation. It doesn't actually manage the leases, since they have been given to the client.
 * Rather, it handles the lease resource which has the type identifier (Entry/Template/Notify) and
 * the expiration time for the lease. LeaseManager groups expirations into Cells, each specifying a
 * given boundary interval.
 *
 * <pre>
 * [ Cell-1 | Cell-2 | Cell-3 | ... | Cell-i ] where each cell holds leases
 * in between intervals:
 * Cell-1 	0...LM_EXPIRATION_TIME_INTERVAL
 * Cell-2	LM_EXPIRATION_TIME_INTERVAL...LM_EXPIRATION_TIME_INTERVAL*2
 * ...
 * Cell-i	LM_EXPIRATION_TIME_INTERVAL...LM_EXPIRATION_TIME_INTERVAL*i
 * </pre>
 *
 * Each Cell is ordered in a Sorted Tree Map which guarantees that the map will be in ascending key
 * order, sorted according to the <i>natural order</i> of the key's class (Long).
 */
@com.gigaspaces.api.InternalApi
public class LeaseManager {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_LEASE);

    private final static long MIN_FORCE_EXPIRATION_INTERVAL = Long.getLong("com.gs.lease-manager.min-force-expiration-interval", 500);

    private final Processor _coreProcessor;
    private final String _spaceName;
    private final SpaceEngine _engine;
    private final SpaceImpl _spaceImpl;
    private final SpaceTypeManager _typeManager;
    private final TransactionHandler _transactionHandler;
    private final com.j_spaces.core.cache.CacheManager _cacheManager;
    private final boolean _dontReapUnderXtnLeases;
    private final FastConcurrentSkipListMap<Long, Cell> _expirationList;
    private final AtomicLong _operationID;
    private final long _clientID;
    private final boolean _slaveLeaseManagerModeConfiguredForEntries;
    private final boolean _alwaysDisableEntriesLeases;
    private final boolean _slaveLeaseManagerModeConfiguredForNotifyTemplates;
    private final long _expirationTimeInterval;
    private final long _backupSpaceLeasesDelay;
    private final int _segmentsPerExpirationCell;
    private final long _expirationTimeRecentDeletes;
    private final long _expirationTimeRecentUpdates;
    private final long _staleReplicaExpirationTime;

    private LeaseReaper _leaseReaperDaemon;
    private boolean _closed;

    private final boolean _supportsRecentExtendedUpdates;


    public LeaseManager(SpaceEngine engine, String spaceName, Processor coreProcessor) {
        _coreProcessor = coreProcessor;
        _spaceName = spaceName;
        _engine = engine;
        _spaceImpl = engine.getSpaceImpl();
        _typeManager = engine.getTypeManager();
        _transactionHandler = engine.getTransactionHandler();
        _cacheManager = engine.getCacheManager();
        _expirationList = new FastConcurrentSkipListMap<Long, Cell>();
        _dontReapUnderXtnLeases = true;
        _operationID = new AtomicLong();
        _clientID = new SecureRandom().nextLong();

        SpaceConfigReader configReader = engine.getConfigReader();

        boolean slaveMode = _engine.getClusterPolicy() != null &&
                _engine.getClusterPolicy().m_Replicated &&
                _engine.getClusterPolicy().getReplicationPolicy().isReplicateLeaseExpirations();

        _alwaysDisableEntriesLeases = configReader.getBooleanSpaceProperty(LM_DISABLE_ENTRIES_LEASES_PROP, String.valueOf(LM_DISABLE_ENTRIES_LEASES_DEFAULT));
        _slaveLeaseManagerModeConfiguredForEntries = slaveMode || _alwaysDisableEntriesLeases;
        _slaveLeaseManagerModeConfiguredForNotifyTemplates = slaveMode;

        _expirationTimeInterval = getLongValue(configReader, LM_EXPIRATION_TIME_INTERVAL_PROP, LM_EXPIRATION_TIME_INTERVAL_DEFAULT);
        _backupSpaceLeasesDelay = getLongValue(configReader, LM_BACKUP_EXPIRATION_DELAY_PROP, LM_BACKUP_EXPIRATION_DELAY_DEFAULT);
        _segmentsPerExpirationCell = _cacheManager.isblobStoreDataSpace() ? 1 : getIntValue(configReader, LM_SEGMEENTS_PER_EXPIRATION_CELL_PROP, LM_SEGMEENTS_PER_EXPIRATION_CELL_DEFAULT);
        _expirationTimeRecentDeletes = getLongValue(configReader, LM_EXPIRATION_TIME_RECENT_DELETES_PROP, LM_EXPIRATION_TIME_RECENT_DELETES_DEFAULT);
        _expirationTimeRecentUpdates = getLongValue(configReader, LM_EXPIRATION_TIME_RECENT_UPDATES_PROP, LM_EXPIRATION_TIME_RECENT_UPDATES_DEFAULT);
        _staleReplicaExpirationTime = getLongValue(configReader, LM_EXPIRATION_TIME_STALE_REPLICAS_PROP, LM_EXPIRATION_TIME_STALE_REPLICAS_DEFAULT);

        _supportsRecentExtendedUpdates = _engine.getCacheManager().isBlobStoreCachePolicy();
        logConfiguration();

    }


    private static long getLongValue(SpaceConfigReader configReader, String spaceProperty, long defaultValue) {
        long result = defaultValue;
        try {
            result = configReader.getLongSpaceProperty(spaceProperty, String.valueOf(defaultValue));

            if (result < 0)
                throw new IllegalArgumentException("Invalid argument for property: " + spaceProperty
                        + " [" + result + "] - must be greater/eq than zero.");
        } catch (Exception ex) {
            result = defaultValue;
            if (_logger.isWarnEnabled()) {
                _logger.warn(LeaseManager.class.getName()
                        + " - Failed to parse "
                        + spaceProperty
                        + "\n using default: "
                        + defaultValue, ex);
            }
        }
        return result;
    }

    private static int getIntValue(SpaceConfigReader configReader, String spaceProperty, int defaultValue) {
        return (int) getLongValue(configReader, spaceProperty, defaultValue);
    }

    private void logConfiguration() {
        if (_logger.isDebugEnabled()) {
            _logger.debug("Lease Manager Reaper will periodically reap expired content of:\n\t"
                    + "Entries/templates - every "
                    + _expirationTimeInterval
                    + " ms\n\t"
                    + "Pending answers - every "
                    + LM_EXPIRATION_TIME_PENDING_ANSWERS_DEFAULT
                    + " ms\n\t"
                    + "Local transactions - every "
                    + _expirationTimeInterval
                    + " ms\n\t"
                    + "Recovered/copied objects - every "
                    + _staleReplicaExpirationTime
                    + " ms\n\t"
                    + "Recently deleted entries - every "
                    + _expirationTimeRecentDeletes
                    + " ms\n\t"
                    + "Recently updated entries - every "
                    + _expirationTimeRecentUpdates
                    + " ms\n\t"
                    + "Transactions of FIFO entries - every "
                    + LM_EXPIRATION_TIME_FIFOENTRY_XTNINFO + " ms\n\t");
        }
    }

    //init the LM- reaper is started
    public synchronized void init() {
        if (_closed)
            return;
        LeaseReaper leaseReaperDaemon = new LeaseReaper(this.getClass().getSimpleName()
                + "$Reaper [" + _spaceName + "]");
        _leaseReaperDaemon = leaseReaperDaemon;
    }

    /* ----------------------------- public API ------------------------------ */

    public void registerEntryLease(IEntryCacheInfo entryCacheInfo, long expiration) {
        register(entryCacheInfo, entryCacheInfo.getEntryHolder(_cacheManager), expiration, ObjectTypes.ENTRY);
    }

    public Lease registerTemplateLease(TemplateCacheInfo templateCacheInfo) {
        final ITemplateHolder template = templateCacheInfo.m_TemplateHolder;
        final long expiration = template.getEntryData().getExpirationTime();
        register(templateCacheInfo, template, expiration, ObjectTypes.NOTIFY_TEMPLATE);
        return new LeaseProxy(expiration, template.getUID(), template.getClassName(), 0, ObjectTypes.NOTIFY_TEMPLATE, _spaceImpl);
    }

    public void reRegisterLease(ILeasedEntryCacheInfo leaseCacheInfo, IEntryHolder entry, long original_expiration,
                                long new_expiration, int objectType) {
        boolean skip_registration = false;
        if (new_expiration == original_expiration)
            skip_registration = true;

        if (!skip_registration && leaseCacheInfo.isConnectedToLeaseManager()) {//check if new && old are in same cell, if so no need to reregister
            long expirationTime_o = original_expiration != Lease.FOREVER ? (((original_expiration / _expirationTimeInterval + 1) * _expirationTimeInterval)) : -1;
            long expirationTime_n = new_expiration != Lease.FOREVER ? (((new_expiration / _expirationTimeInterval + 1) * _expirationTimeInterval)) : -1;
            skip_registration = expirationTime_o == expirationTime_n; //same cell
        }

        if (!skip_registration) {
            unregister(leaseCacheInfo, original_expiration);
            register(leaseCacheInfo, entry, new_expiration, objectType);
        }
    }

    public OperationID createOperationIDForLeaseExpirationEvent() {
        return new OperationID(_clientID, _operationID.incrementAndGet());
    }

    public boolean isNoReapUnderXtnLeases() {
        return _dontReapUnderXtnLeases;
    }

    private long getBackupSpaceLeasesDelay() {
        return _backupSpaceLeasesDelay;
    }

    public long getEffectiveEntryLeaseTime(long time) {
        return isSlaveLeaseManagerForEntries() ? Long.MIN_VALUE :
                (_spaceImpl.isBackup() ? time - getBackupSpaceLeasesDelay() : time);
    }

    public long getEffectiveEntryLeaseTimeForReaper(long time) {
        return isSlaveLeaseManagerForEntries() ? time :
                (_spaceImpl.isBackup() ? time - getBackupSpaceLeasesDelay() : time);
    }

    public boolean isCurrentLeaseReaperThread() {
        LeaseReaper leaseReaperDaemon = _leaseReaperDaemon;
        if (leaseReaperDaemon == null) {
            synchronized (this) {
                leaseReaperDaemon = _leaseReaperDaemon;
            }
        }
        return Thread.currentThread() == leaseReaperDaemon;
    }

    /**
     * Force a lease reaper cycle and blocks until the cycle is completed (unless {@link
     * LeaseReaper#_minForceReapInterval} is not elapsed since last reaper cycle completion)
     *
     * @param mustExecute TODO XML:
     * @param timeToWait  indicates how long should the leaseReaperDaemon wait for completing the
     *                    cycle, 0 means forever
     */
    public void forceLeaseReaperCycle(boolean mustExecute, long timeToWait) throws InterruptedException {
        LeaseReaper leaseReaperDaemon = _leaseReaperDaemon;
        if (leaseReaperDaemon == null) {
            synchronized (this) {
                leaseReaperDaemon = _leaseReaperDaemon;
            }
        }
        if (leaseReaperDaemon == null)
            return;
        long currentCycle = leaseReaperDaemon.getCurrentCycle();
        if (leaseReaperDaemon.forceCycle(mustExecute))
            leaseReaperDaemon.waitForCycleCompletion(currentCycle, timeToWait);
    }

    public void forceLeaseReaperCycle(boolean mustExecute) throws InterruptedException {
        forceLeaseReaperCycle(mustExecute, 0l);
    }

    /**
     * Renews a lease for an additional period of time (specified in milliseconds) for a registered
     * entry in this Lease Manager. This duration is not added to the original lease, but is used to
     * determine a new expiration time for the existing lease. If the renewal is granted this is
     * reflected in value returned by getExpiration. If the renewal fails, the lease is left intact
     * for the same duration that was in force prior to the call to renew.
     *
     * @param entryUid        Uid of entry to renew a lease for.
     * @param className       The class name of this entry.
     * @param objectType      The type of the entry.
     * @param duration        The requested duration in milliseconds.
     * @param fromReplication <tt>true</tt> If this entry arrived from replication.
     * @return duration the requested duration in milliseconds
     * @throws UnknownLeaseException  if lease is unknown to the registrar.
     * @throws InternalSpaceException if an internal error occurred.
     */
    public long renew(String entryUid, String className, int objectType,
                      long duration, boolean fromReplication, boolean origin, boolean isFromGateway)
            throws UnknownLeaseException, InternalSpaceException {
        try {
            extendLeasePeriod(entryUid,
                    className,
                    objectType,
                    duration,
                    fromReplication,
                    origin,
                    false,
                    null /* operationID */,
                    isFromGateway);
        } catch (UnknownLeaseException unknownLeaseException) {
            if (_cacheManager.isCacheExternalDB() && objectType != ObjectTypes.NOTIFY_NULL_TEMPLATE && objectType != ObjectTypes.NOTIFY_TEMPLATE) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(
                            "Failed to renew lease of an entry belonging to external-data-source.",
                            unknownLeaseException);
                }
            } else
                throw unknownLeaseException;
        }

        return duration;
    }

    public void update(String entryUid, String className, int objectType, long duration)
            throws UnknownLeaseException, InternalSpaceException {
        final boolean fromReplication = false;
        final boolean origin = true;
        final boolean isFromGateway = false;
        if (duration == LeaseUtils.DISCARD_LEASE)
            cancel(entryUid, className, objectType, fromReplication, origin, isFromGateway);
        else
            renew(entryUid, className, objectType, duration, fromReplication, origin, isFromGateway);
    }

    /**
     * Batch renewal operations for entries.
     *
     * @param entryUids   Uid of entries to renew a lease for.
     * @param classNames  The class name of each corresponding entry.
     * @param objectTypes Type of the object of each entry.
     * @param durations   The requested durations (in milliseconds) of each entry.
     * @return an Object[] { exceptions, newDurations }
     * @see #renew(String, String, int, long, boolean, boolean, boolean)
     */
    public Object[] renewAll(String[] entryUids, String[] classNames,
                             int[] objectTypes, long[] durations) {
        Exception[] exceptions = new Exception[entryUids.length];
        long[] newDurations = new long[entryUids.length];

        for (int i = 0; i < entryUids.length; i++) {
            try {
                newDurations[i] = renew(entryUids[i],
                        classNames[i],
                        objectTypes[i],
                        durations[i],
                        false /* fromReplication */,
                        true /*origin*/,
                        false /* fromGateway */);
            } catch (Exception ex) {
                exceptions[i] = ex;
            }
        }

        return new Object[]{exceptions, newDurations};
    }

    /**
     * Cancel a lease for a registered Entry in this Lease Manager.
     *
     * @param entryUid        Uid of entry to renew a lease for.
     * @param classname       The class name of this entry.
     * @param objectType      The type of the entry.
     * @param fromReplication <tt>true</tt> If this entry arrived from replication.
     * @return IEntryHolder
     */
    public IEntryHolder cancel(String entryUid, String classname, int objectType,
                               boolean fromReplication, boolean origin, boolean isFromGateway)
            throws UnknownLeaseException {
        return cancel(entryUid, classname, objectType,
                fromReplication, origin, false /*lease_expired*/, null /*operationID*/, isFromGateway);
    }

    public IEntryHolder cancel(String entryUid, String classname, int objectType,
                               boolean fromReplication, boolean origin, boolean lease_expired, OperationID operationID, boolean isFromGateway)
            throws UnknownLeaseException {
        try {
            return extendLeasePeriod(entryUid,
                    classname,
                    objectType,
                    LeaseUtils.DISCARD_LEASE /* duration */,
                    fromReplication,
                    origin,
                    lease_expired,
                    operationID,
                    isFromGateway);
        } catch (UnknownLeaseException unknownLeaseException) {
            if (_cacheManager.isCacheExternalDB()) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(
                            "Failed to cancel lease of an entry belonging to external-data-source.",
                            unknownLeaseException);
                }
                return null;
            } else
                throw unknownLeaseException;
        }
    }

    /**
     * Batch cancellation operations for entries.
     *
     * @param entryUids   Uid of entries to cancel their lease.
     * @param classNames  The class name of each corresponding entry.
     * @param objectTypes Type of the object of each entry.
     * @return a non-null Exception[] array; might be empty.
     * @see #cancel(String, String, int, boolean, boolean, boolean)
     */
    public Exception[] cancelAll(String[] entryUids, String[] classNames,
                                 int[] objectTypes) {
        ArrayList<Exception> exceptionsList = new ArrayList<Exception>(0);

        for (int i = 0; i < entryUids.length; i++) {
            try {
                cancel(entryUids[i],
                        classNames[i],
                        objectTypes[i],
                        false /* fromReplication */,
                        true /*origin*/,
                        false /* fromGateway */);
            } catch (Exception ex) {
                exceptionsList.add(ex);
            }
        }

        return exceptionsList.toArray(new Exception[exceptionsList.size()]);
    }

    /**
     * Closes the Lease Manager by waking up the lease reaper daemon thread and waits for it to
     * gracefully die.
     */
    public synchronized final void close() {
        _closed = true;
        if (_leaseReaperDaemon != null)
            _leaseReaperDaemon.clean();
    }

    /**
     * Calculates the absolute time for the given duration. Handles Lease.FOREVER, Lease.ANY, and
     * NO_WAIT. Same {@link #toAbsoluteTime(long)} but uses a provided clock.
     *
     * @param duration duration in milliseconds.
     * @param current  current time.
     * @return The absolute time of the requested given duration.
     */
    public static final long toAbsoluteTime(long duration, long current) {
        if (duration == Lease.FOREVER || duration == Lease.ANY)
            return Long.MAX_VALUE;

        if (duration == 0)
            return 0;

        duration += current;

        if (duration < 0)
            return Long.MAX_VALUE;

        return duration;
    }

    /**
     * Calculates the absolute time for the given duration. Handles Lease.FOREVER, Lease.ANY, and
     * NO_WAIT. Same {@link #toAbsoluteTime(long)} but uses a provided clock.
     *
     * @param duration duration in milliseconds.
     * @return The absolute time of the requested given duration.
     */
    public static final long toAbsoluteTime(long duration) {
        if (duration == Lease.FOREVER || duration == Lease.ANY)
            return Long.MAX_VALUE;

        if (duration == 0)
            return 0;

        duration += SystemTime.timeMillis();

        if (duration < 0)
            return Long.MAX_VALUE;

        return duration;
    }

    //is this LM operating in slave mode for entries?
    public boolean isSlaveLeaseManagerForEntries() {
        return _alwaysDisableEntriesLeases || (_slaveLeaseManagerModeConfiguredForEntries && !_spaceImpl.isPrimary());
    }

    //is this LM operating in slave mode for notify templates?
    public boolean isSlaveLeaseManagerForNotifyTemplates() {
        return _slaveLeaseManagerModeConfiguredForNotifyTemplates && !_spaceImpl.isPrimary();
    }

    public boolean replicateLeaseExpirationEventsForEntries() {
        return _slaveLeaseManagerModeConfiguredForEntries && _spaceImpl.isPrimary();
    }

    public boolean replicateLeaseExpirationEventsForNotifyTemplates() {
        return _slaveLeaseManagerModeConfiguredForNotifyTemplates && _spaceImpl.isPrimary();
    }

    public boolean isSupportsRecentExtendedUpdates() {
        return _supportsRecentExtendedUpdates;
    }

    /* ----------------------------- private API ------------------------------ */

    /**
     * Notifies the manager of a new lease being created.
     *
     * @param entry      Entry associated with the new Lease.
     * @param expiration The duration of the lease (in milliseconds) as absolute time.
     * @param objectType The type of the Entry object.
     */
    private void register(ILeasedEntryCacheInfo leaseCacheInfo, IEntryHolder entry, long expiration, int objectType) {
        boolean skipCellRegistration = expiration == Lease.FOREVER || (_alwaysDisableEntriesLeases && objectType == ObjectTypes.ENTRY);
        if (!skipCellRegistration) {
            Long expirationTime = ((expiration / _expirationTimeInterval + 1) * _expirationTimeInterval);

            while (true) {
                Cell cell = _expirationList.get(expirationTime);

                if (cell == null) {
                    cell = new Cell(_segmentsPerExpirationCell, expirationTime);
                    Cell currCell = _expirationList.putIfAbsent(expirationTime, cell);
                    if (currCell != null)
                        cell = currCell;
                    cell.register(leaseCacheInfo, entry, objectType);
                } else {
                    cell.register(leaseCacheInfo, entry, objectType);
                }
                //if cell is marked as cleaned, reinsert it in order to avoid
                //skippimg the entry. (note- the indicator is not volatile)
                if (cell.isCleaned()) {
                    synchronized (cell) {
                        Object cur = _expirationList.putIfAbsent(cell.getCellKey(), cell);
                        if (cur == cell || cur == null)
                            break;
                    }
                } else
                    break;
            }
        } else
            leaseCacheInfo.setLeaseManagerListRefAndPosition(null, null);
    }

    /**
     * unregister from lease manager based on direct backrefs Note: entry/template must be locked
     */
    public void unregister(ILeasedEntryCacheInfo leaseCacheInfo, long expiration) {
        boolean unregister;
        if (leaseCacheInfo.isBlobStoreEntry())
            unregister = expiration != Long.MAX_VALUE && !_alwaysDisableEntriesLeases;
        else
            unregister = leaseCacheInfo.isConnectedToLeaseManager();

        if (unregister) {
            if (!leaseCacheInfo.isBlobStoreEntry()) {
                leaseCacheInfo.getLeaseManagerListRef().remove(leaseCacheInfo.getLeaseManagerPosition());
            } else {//need to remove from cell
                Long expirationTime = ((expiration / _expirationTimeInterval + 1) * _expirationTimeInterval);
                Cell cell = _expirationList.get(expirationTime);
                if (cell != null)
                    cell.unregisterByPos(leaseCacheInfo.getLeaseManagerPosition(), true /*isEntry*/);
            }
            leaseCacheInfo.setLeaseManagerListRefAndPosition(null, null);
        }
    }

    /**
     * Returns an extended lease period for the entry represented by its Uid. The Entry expiration
     * time is updated to an extended absolute time.
     *
     * @return An IEntryHolder with an extended expiration time.
     * @throws UnknownLeaseException  if lease is unknown to the registrar.
     * @throws InternalSpaceException if an internal error occurred.
     */
    private final IEntryHolder extendLeasePeriod(String entryUid,
                                                 String className, int objectType, long duration,
                                                 boolean fromReplication, boolean origin, boolean leaseExpired, OperationID operationID, boolean isFromGatway)
            throws UnknownLeaseException, InternalSpaceException {
        IEntryHolder entry = null;
        Context context = null;
        ILockObject entryLock = null;
        boolean non_evictable = false;
        boolean cancelLease = (duration == LeaseUtils.DISCARD_LEASE);
        long original_time = 0;
        try {
            context = _cacheManager.getCacheContext();
            context.setFromReplication(fromReplication);
            context.setOperationID(operationID);
            context.setFromGateway(isFromGatway);
            long currentTime = SystemTime.timeMillis();
            long timeToCheck = getEffectiveEntryLeaseTime(currentTime);

            if (!_cacheManager.isEvictableCachePolicy() || objectType != ObjectTypes.ENTRY) {
                entry = objectType == ObjectTypes.ENTRY ? _cacheManager
                        .getEntry(context,
                                entryUid,
                                className,
                                null /* selectiontemplate */,
                                false /* tryInsertToCache */,
                                false /* lockeEntry */,
                                true /* useOnlyCache */)
                        : _cacheManager
                        .getTemplate(entryUid);

                non_evictable = true;
                if (entry == null || entry.isExpired(timeToCheck)) {
                    String reason = "Failed to "
                            + getExtendLeasePeriodDescription(leaseExpired,
                            cancelLease)
                            + " entry's lease of class "
                            + className

                            + " and uid "
                            + entryUid
                            + (entry == null ? " as it is no longer present in space "
                            : "")
                            + " (may have been already expired or canceled).";
                    throw new UnknownLeaseException(reason);
                }
            }

            boolean extended = false;
            entryLock = non_evictable ? _cacheManager
                    .getLockManager()
                    .getLockObject(entry,
                            !non_evictable /* isEvictable */)
                    : _cacheManager
                    .getLockManager()
                    .getLockObject(entryUid);

            synchronized (entryLock) {
                try {
                    if (_cacheManager.isEvictableCachePolicy() && objectType == ObjectTypes.ENTRY) {
                        if (cancelLease) {//in case expiration is only from eviction-
                            if (_cacheManager.requiresEvictionReplicationProtection() && _cacheManager.getEvictionReplicationsMarkersRepository().isEntryEvictable(entryUid, false /*alreadyLocked*/))
                                throw new UnknownLeaseException("entry in markers repository- cannot be cancelled.");
                            IEntryCacheInfo pe = _cacheManager.getPEntryByUid(entryUid);
                            if (pe == null) {
                                String reason = "Failed to "
                                        + getExtendLeasePeriodDescription(leaseExpired,
                                        cancelLease)
                                        + " entry's lease of class "
                                        + className
                                        + " and uid "
                                        + entryUid
                                        + (entry == null ? " as it is no longer present in space "
                                        : "")
                                        + " (may have been already expired canceled or evicted).";
                                throw new UnknownLeaseException(reason);
                            }

                            if (_engine.isExpiredEntryStayInSpace(pe.getEntryHolder(_cacheManager)) && pe.isPinned())
                                throw new UnknownLeaseException("entry is pinned- cannot be cancelled.");
                        }


                        // external DB cache: lease manipulations are only
                        // performed in memory, lease
                        // is the duration in cache, not existence in DB
                        entry = _cacheManager
                                .getEntry(context,
                                        entryUid,
                                        className,
                                        null /* selection template */,
                                        true /* tryInsertToCache */,
                                        true /* lockeEntry */,
                                        _cacheManager
                                                .isCacheExternalDB() /* useOnlyCache */);
                    } else {
                        if (entry.isBlobStoreEntry()) {//bring the full version
                            entry = _cacheManager
                                    .getEntry(context,
                                            entry,
                                            true /* tryInsertToCache */,
                                            true /* lockeEntry */,
                                            true /* useOnlyCache */);
                        }
                    }

                    // renew/cancel only if the entry lease hasn't expired yet
                    // or
                    // deleted by LeaseRenewalManager reaper
                    if (entry == null || entry.isDeleted()
                            || entry.isExpired(timeToCheck)) {
                        String reason = "Failed to " + getExtendLeasePeriodDescription(leaseExpired, cancelLease) + " entry's lease " +
                                (entry == null ? " as it is no longer present in space " : (entry.isDeleted() ? " as it is already deleted inside the space " : "")) +
                                "(entry may have been already expired or canceled).";
                        throw new UnknownLeaseException(reason);
                    }
                    if (objectType == ObjectTypes.ENTRY
                            && entry.getWriteLockOwner() != null) {
                        XtnStatus status = entry.getWriteLockOwner()
                                .getStatus();
                        if (status != XtnStatus.COMMITED
                                && status != XtnStatus.ROLLED)
                            throw new UnknownLeaseException("Failed to " + getExtendLeasePeriodDescription(leaseExpired, cancelLease) + " entry's lease, entry is write-locked under an ongoing transaction.");
                    }

                    boolean shouldReplicate = false;

                    if (entry instanceof NotifyTemplateHolder) {
                        NotifyTemplateHolder template = (NotifyTemplateHolder) entry;

                        XtnEntry xtnEntry = template.getXidOriginated();

                        if (xtnEntry != null && !xtnEntry.m_Active)
                            throw new UnknownLeaseException("Failed to " + getExtendLeasePeriodDescription(leaseExpired, cancelLease) + " template's lease: Non-active transaction.");


                        if (_engine.isReplicated() && !fromReplication) {
                            shouldReplicate = template.isReplicateNotify();
                        }

                        if (cancelLease) {
                            _cacheManager.removeTemplate(context, template,
                                    fromReplication, origin, false,
                                    leaseExpired ? SpaceEngine.TemplateRemoveReasonCodes.LEASE_EXPIRED : SpaceEngine.TemplateRemoveReasonCodes.LEASE_CANCEL);

                            return template;
                        }
                    } else
                    // non-template
                    {

                        boolean ofReplicableClass = false;

                        if (_engine.isReplicated()) {
                            IServerTypeDesc typeDesc = _typeManager.getServerTypeDesc(entry.getClassName());
                            ReplicationOperationType oper = cancelLease ? ReplicationOperationType.LEASE_EXPIRATION : ReplicationOperationType.EXTEND_LEASE;
                            shouldReplicate = _engine.shouldReplicate(oper, typeDesc, false, fromReplication);
                            ofReplicableClass = typeDesc.getTypeDesc().isReplicable();
                        }

                        if (cancelLease) {
                            _engine.removeEntrySA(context,
                                    entry,
                                    fromReplication,
                                    origin,
                                    ofReplicableClass /* ofReplicatableClass */,
                                    leaseExpired ? SpaceEngine.EntryRemoveReasonCodes.LEASE_EXPIRED : SpaceEngine.EntryRemoveReasonCodes.LEASE_CANCEL /* fromLeaseExpiration */,
                                    false /* disableReplication */,
                                    false /* disableProcessorCall */,
                                    false /*disableSADelete*/);

                            return entry;
                        }
                    }
                    original_time = entry.getEntryData().getExpirationTime();
                    //translate duration to absolute time
                    entry.setExpirationTime(toAbsoluteTime(duration));

                    //in case of off-heap, signal as dirty
                    if (entry.isBlobStoreEntry())
                        ((IBlobStoreEntryHolder) entry).setDirty(_cacheManager);


                    _cacheManager
                            .extendLeasePeriod(context,
                                    entry.getEntryData()
                                            .getExpirationTime(),
                                    original_time,
                                    entryUid,
                                    className,
                                    objectType,
                                    shouldReplicate,
                                    origin);
                    extended = true;
                } finally {
                    if (extended
                            && objectType == ObjectTypes.ENTRY
                            && _cacheManager.mayNeedEntriesUnpinning())
                        _cacheManager
                                .unpinIfNeeded(context, entry, null /* template */, null /* pEntry */);
                }
            }//synchronized (entryLock)
        } catch (SAException ex) {
            JSpaceUtilities.throwLeaseInternalSpaceException(ex.toString(), ex);
        } finally {
            if (entryLock != null)
                _cacheManager
                        .getLockManager()
                        .freeLockObject(entryLock);

            if (context != null) {
                try {
                    /** perform sync-replication */
                    _engine.performReplication(context);
                } finally {
                    _cacheManager.freeCacheContext(context);
                }
            }// if
        }// finally

        return entry;
    }

    /**
     * called as part of demote from primary to backup in order to verify LM is not active
     * @return true if no cycle false if timeout pqassed and cycle still busy
     */

    public boolean waitForNoCycleOnQuiesce(long timeout)
    {
        return
                _leaseReaperDaemon.waitForNoCycleOnQuiesce(timeout);
    }

    private static String getExtendLeasePeriodDescription(boolean leaseExpired, boolean cancelLease) {
        return cancelLease ? (leaseExpired ? "expire" : "cancel") : "renew";
    }

    /**
     * Lease harvester daemon thread. Wakes up every <tt>LM_EXPIRATION_TIME_INTERVAL</tt> to harvest
     * expired leases and cleanup obsolete data structures.
     */
    private final class LeaseReaper extends GSThread {
        private boolean _shouldDie;
        private long _nextExpirationTimeInterval = SystemTime.timeMillis();

        /*
         * different reap intervals (assumption is that
         * LM_EXPIRATION_TIME_INTERVAL is always smaller then other intervals,
         * otherwise intervals are equivalent.)
         */
        private long _lastReapedSpaceContentObjects;
        private long _lastRepeadRecentDeletes;
        private long _lastReapedRecentUpdates;
        private long _lastReapedUnusedXtn;
        private long _lastReapedMarkersRepository;
        private final Object _cycleLock = new Object();
        private long _cycleCount;
        private long _lastCycleEnded;
        private boolean _force;
        private volatile boolean _inCycle;

        /**
         * LeaseReaper daemon thread.
         *
         * @param threadName the name of this daemon thread.
         */
        public LeaseReaper(String threadName) {
            super(threadName);
            this.setDaemon(true);
            this.start();
        }

        @Override
        public void run() {

            try {
                while (!isInterrupted()) {
                    try {
                        boolean skippedReapingInQuiesceMode = false;
                        fallAsleep(skippedReapingInQuiesceMode);

                        if (_shouldDie)
                            break;

                        if (_logger.isTraceEnabled()) {
                            _logger.trace(this.getName()
                                    + " - woke up for reaping.");
                        }
                        if (isSpaceInQuiesce(true))
                            skippedReapingInQuiesceMode = true;
                        if (!skippedReapingInQuiesceMode) {
                            _inCycle = true;
                            //recheck after signaling incycle
                            if (isSpaceInQuiesce(true))
                                skippedReapingInQuiesceMode = true;
                        }
                        if (!skippedReapingInQuiesceMode) {
                            reapExpiredEntries();
                            reapExpiredXtns();
                            reapPhantomGlobalXtns();
                            reapStaleReplicas();
                            reapRecentDeletes();
                            reapRecentUpdates();
                            reapFifoXtnsEntryInfo();
                            reapReachedMarkers();
                            reapStuck2PCPreparedXtns();
                            reapRecentExtendedUpdates();
                            reapExpiredIterators();
                            //TODO reap expired iterators
                        }
                        reapUnusedXtns();
                        signalEndCycle();
                    } catch (Exception e) {
                        if (_logger.isErrorEnabled()) {
                            _logger.error(this.getName()
                                    + " - caught Exception", e);
                        }
                    }
                    finally
                    {
                        if (_inCycle)
                        {
                            synchronized(_cycleLock)
                            {
                                _inCycle=false;
                                _cycleLock.notifyAll();
                            }
                        }
                    }
                }
            } finally {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(this.getName() + " terminated.");
                }
            }
        }

        private boolean isSpaceInQuiesce(boolean useLogger) {
            if (_spaceImpl.getQuiesceHandler() != null && _spaceImpl.getQuiesceHandler().isOn()) {//space in quiesce mode dont reap and harvest
                if (useLogger && _logger.isTraceEnabled()) {
                    _logger.trace(this.getName()
                            + " - is not reaping since space in quiesce mode.");
                }
                return true;
            }
            return false;
        }

        public boolean forceCycle(boolean mustExecute) {
            synchronized (_cycleLock) {
                if (!mustExecute && SystemTime.timeMillis() - _lastCycleEnded < MIN_FORCE_EXPIRATION_INTERVAL)
                    return false;
                //Signal force state
                _force = true;
            }
            synchronized (this) {
                notify();
                return true;
            }
        }

        public void waitForCycleCompletion(long currentCycle, long timeToWait) throws InterruptedException {
            synchronized (_cycleLock) {
                while (_cycleCount == currentCycle && !_shouldDie) {
                    if (timeToWait != 0) {
                        _cycleLock.wait(timeToWait);
                        return;
                    } else
                        _cycleLock.wait();
                }

            }
        }

        public long getCurrentCycle() {
            synchronized (_cycleLock) {
                return _cycleCount;
            }
        }

        private void signalEndCycle() {
            synchronized (_cycleLock) {
                _inCycle = false;
                _force = false;
                _cycleCount++;
                _lastCycleEnded = SystemTime.timeMillis();
                _cycleLock.notifyAll();
            }
        }

        /**
         * falls asleep for <tt>LM_EXPIRATION_TIME_INTERVAL</tt> repeatedly maintaining a
         * <i>fixed-rate execution</i>. <p> In fixed-rate execution, each execution is scheduled
         * relative to the scheduled execution time of the initial execution.  If an execution is
         * delayed for any reason (such as garbage collection or other background activity), two or
         * more executions will occur in rapid succession to "catch up." In the long run, the
         * frequency of execution will be exactly the reciprocal of the specified period (assuming
         * the system clock underlying <tt>Object.wait(long)</tt> is accurate).
         */
        protected final void fallAsleep(boolean skippedReapingInQuiesceMode) {
            synchronized (this) {
                try {
                    if (!_shouldDie) {
                        _nextExpirationTimeInterval = skippedReapingInQuiesceMode ? (_nextExpirationTimeInterval + (_expirationTimeInterval / 10)) : (_nextExpirationTimeInterval + _expirationTimeInterval);
                        long fixedRateDelay = _nextExpirationTimeInterval - SystemTime.timeMillis();
                        if (fixedRateDelay <= 0) {
                            if (_logger.isTraceEnabled())
                                _logger.trace("Skipped fallAsleep since fixedRateDelay=" + fixedRateDelay);
                            _nextExpirationTimeInterval = SystemTime.timeMillis();
                            return;
                        }

                        if (_logger.isTraceEnabled())
                            _logger.trace("fallAsleep - going to wait fixedRateDelay=" + fixedRateDelay);
                        wait(fixedRateDelay);
                        if (_force) {
                            if (_logger.isTraceEnabled())
                                _logger.trace("lease reaper was forcibly waken up");
                            _nextExpirationTimeInterval = SystemTime.timeMillis();
                        }

                    }
                } catch (InterruptedException ie) {

                    if (_logger.isTraceEnabled()) {
                        _logger.trace(this.getName()
                                + " interrupted.", ie);
                    }

                    _shouldDie = true;

                    signalEndCycle();
                    //Restore the interrupted status
                    interrupt();
                }
            }
        }

        /**
         * Wakes up, waits for reaper to die, and clears all lease mappings.
         */
        protected final void clean() {
            if (!isAlive())
                return;

            synchronized (this) {
                _shouldDie = true;
                notify();
            }

            // wait for termination
            try {
                join();
            } catch (InterruptedException e) {
            }

            if (_expirationList != null)
                _expirationList.clear();
        }

        /**
         * called as part of demote from primary to backup in order to verify LM is not active
         * @return true if no cycle false if timeout pqassed and cycle still busy
         */

        boolean waitForNoCycleOnQuiesce(long timeout)
        {
            //TBD LOGGER MESSAGES!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11
            if (timeout == 0)
                throw new RuntimeException("timeout must be supplied!");
            if (!isSpaceInQuiesce(false))
                throw new RuntimeException("space not in quiesce mode!");

            if (!_inCycle)
                return true;
            synchronized(_cycleLock)
            {
                if (!_inCycle)
                    return true;

                try {
                    _cycleLock.wait(timeout);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return !_inCycle;
            }
        }


        /* --------------------------- Reaper tasks -------------------------- */


        /**
         * Cleans expired entries, every <tt>LM_EXPIRATION_TIME_INTERVAL</tt>.
         */
        private static final int DETACH_LIMIT_TO_REPORT = 1000;

        private final void reapExpiredEntries() {
            Context context = null;
            int reapCount = 0;
            int detached = 0;
            if (_expirationList.isEmpty())
                return;
            boolean reached_last_cell = false;

            try {
                Iterator iter = _expirationList.values().iterator();

                while (iter.hasNext()) {
                    if (reached_last_cell)
                        break;

                    long currentTime = getEffectiveEntryLeaseTimeForReaper(SystemTime.timeMillis());

                    Cell cell = (Cell) iter.next();
                    Long expirationTime = cell.getCellKey();

                    if (expirationTime.longValue() > currentTime) {
                        if (!_force)
                            break;
                        reached_last_cell = true;
                    }

                    ILockObject entryLock = null;
                    Iterator<IEntryHolder> entriesUids = !isSlaveLeaseManagerForEntries() ? cell.mateExpriedEntriesUidsIter(_engine) : null;
                    Iterator<IEntryHolder> n_templatesUids = cell.mateExpriedNotifyTemplatesUidsIter();
                    Iterator<IEntryHolder> currentIter = entriesUids != null ? entriesUids : n_templatesUids;

                    if (context == null)
                        context = _cacheManager.getCacheContext();

                    if (_engine.isSyncReplicationEnabled() && _slaveLeaseManagerModeConfiguredForEntries)
                        context.setSyncReplFromMultipleOperation(true);


                    for (; ; ) {
                        boolean more = currentIter == null ? false : currentIter.hasNext();
                        if (!more) {
                            if (currentIter == entriesUids && n_templatesUids != null) {
                                currentIter = n_templatesUids;
                                if (currentIter != null)
                                    more = currentIter.hasNext();
                            }
                        }
                        if (!more)
                            break;
                        boolean isEntry = currentIter == entriesUids;
                        IEntryHolder iter_entry = currentIter.next();
                        if (iter_entry == null) {
                            if (isEntry && _cacheManager.isblobStoreDataSpace())
                                detached++; //in off heap we can't get a "deleted" entry in case of detached
                            continue;
                        }

                        IEntryHolder entry = (isEntry && !iter_entry.isBlobStoreEntry()) ? _cacheManager.getEntryByUidFromPureCache(iter_entry.getUID()) :
                                iter_entry;

                        if (entry == null) {
                            if (iter_entry.isDeleted())
                                detached++;
                            continue;
                        }

                        boolean non_evictable = (!_cacheManager.isEvictableCachePolicy() || !isEntry);

                        entryLock = _cacheManager
                                .getLockManager()
                                .getLockObject(entry, !non_evictable/* isEvictable */);

                        boolean removedEntry = false;

                        try {
                            boolean needUnpin = false;
                            IEntryHolder cachedEntry = null;
                            synchronized (entryLock) {
                                try {
                                    if (!isEntry) {
                                        if (entry.isDeleted())
                                            continue; // already deleted
                                        if (!entry.isExpired(currentTime)) {
                                            continue; // not relevant any more
                                        }
                                        if (isSlaveLeaseManagerForNotifyTemplates() && ((NotifyTemplateHolder) entry).isReplicateNotify())
                                            continue;  //slave mode for notify templates

                                        context.setOperationID(createOperationIDForLeaseExpirationEvent());

                                        _cacheManager.removeTemplate(context, (ITemplateHolder) entry,
                                                false /* fromRepl */,
                                                true /*origin*/,
                                                !(replicateLeaseExpirationEventsForNotifyTemplates() && ((NotifyTemplateHolder) entry).isReplicateNotify()) /*dontReplicate*/,
                                                TemplateRemoveReasonCodes.LEASE_EXPIRED);
                                    } else {
                                        // verify by getting and checking under
                                        // lock
                                        Context ctx = _cacheManager.getCacheContext();
                                        try {
                                            if (_cacheManager.isEvictableCachePolicy()) {
                                                // fix GS-13590
                                                if (_cacheManager.requiresEvictionReplicationProtection() && !entry.isTransient() && !_cacheManager.getEvictionReplicationsMarkersRepository().isEntryEvictable(entry.getUID(), false /*alreadyLocked*/))
                                                    continue; //markers repository- entry cannot be evicted

                                                IEntryCacheInfo pe = null;
                                                if (_engine.isExpiredEntryStayInSpace(entry)) {//in case expiration is only from eviction-
                                                    pe = _cacheManager.getPEntryByUid(entry.getUID());
                                                    cachedEntry = pe != null ? pe.getEntryHolder(_cacheManager) : null;
                                                    if (pe.isPinned())
                                                        continue;
                                                }

                                                cachedEntry = _cacheManager
                                                        .getEntry(ctx,
                                                                entry,
                                                                true /* tryInsertToCache */,
                                                                true /* lockedEntry */, _engine.isExpiredEntryStayInSpace(entry) /*useOnlyCache*/);
                                                if (cachedEntry != null)
                                                    entry = cachedEntry;
                                                else
                                                    continue; // entry not valid any more
                                            } else {
                                                if (entry.isBlobStoreEntry()) {//bring the full version
                                                    entry = _cacheManager
                                                            .getEntry(context,
                                                                    entry,
                                                                    true /* tryInsertToCache */,
                                                                    true /* lockeEntry */,
                                                                    true /* useOnlyCache */);
                                                }
                                            }

                                            if (entry.isDeleted())
                                                continue; // already deleted

                                            if (!entry.isExpired(currentTime)) {
                                                needUnpin = true;
                                                continue; // not relevant any
                                                // more
                                            }
                                            if (isNoReapUnderXtnLeases() && entry.isEntryUnderWriteLockXtn()) {
                                                needUnpin = true;
                                                continue; // writelocked under xtn- dot reap it
                                            }

                                        } finally {
                                            _cacheManager.freeCacheContext(ctx);
                                        }
                                        IServerTypeDesc typeDesc = _typeManager.getServerTypeDesc(entry.getClassName());
                                        context.setOperationID(createOperationIDForLeaseExpirationEvent());
                                        _engine.removeEntrySA(context, entry, typeDesc,
                                                false /* fromRepl */,
                                                true /* origin */,
                                                SpaceEngine.EntryRemoveReasonCodes.LEASE_EXPIRED /*fromLeaseExpiration*/,
                                                !replicateLeaseExpirationEventsForEntries() /* disableReplication */,
                                                true /* disableProcessorCall */,
                                                false /* disableSADelete */);

                                        removedEntry = true;
                                    }//entry
                                } finally {
                                    //while entry still locked
                                    if (needUnpin
                                            && _cacheManager.mayNeedEntriesUnpinning())
                                        _cacheManager
                                                .unpinIfNeeded(context, entry,
                                                        null,
                                                        null /* pEntry */);

                                }
                            } /* synchronized(entryLock) */

                            reapCount++;
                        } finally {
                            if (entryLock != null) {
                                _cacheManager
                                        .getLockManager()
                                        .freeLockObject(entryLock);
                                entryLock = null;
                            }
                        }

                        //was entry removed? call direct processor
                        //performed out of lock!!!!
                        if (removedEntry) {
                            try {
                                _coreProcessor.handleEntryExpiredCoreSA(entry, null/* xtn */, false);
                            } catch (Exception ex) {
                                if (_logger.isErrorEnabled()) {
                                    _logger.error(
                                            this.getName()
                                                    + " - failed while handling expiration of entry.",
                                            ex);
                                }

                                reapCount--;
                            }
                        }

                        if (context != null &&
                                context.getReplicationContext() != null &&
                                !_spaceImpl.isBackup() &&
                                _slaveLeaseManagerModeConfiguredForEntries) {
                            ReplicationPolicy replicationPolicy = _engine.getClusterPolicy().getReplicationPolicy();
                            int multiOpChunkSize = replicationPolicy.m_SyncReplPolicy.getMultipleOperationChunkSize();
                            if (multiOpChunkSize != -1 && reapCount >= multiOpChunkSize) {
                                _engine.performReplication(context); //batch replication
                                if (_logger.isDebugEnabled())
                                    _logger.debug(this.getName() + " - Reaped expired leases. [Reaped: " + reapCount + "]");
                                reapCount = 0;
                            }
                        }

                    }//for(;;)
                }//for

            } catch (Exception ex) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(
                            this.getName()
                                    + " - caught exception while reaping expired entries.",
                            ex);
                }
            } finally { // graceful shutdown of reaper

                if (context != null) {
                    try {
                        if (reapCount > 0 && _slaveLeaseManagerModeConfiguredForEntries && !_spaceImpl.isBackup()) {
                            _engine.performReplication(context); //batch replication
                        }
                    } finally {
                        _cacheManager.freeCacheContext(context);
                        context = null;
                    }
                }
            }

            if (reapCount > 0) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(this.getName()
                            + " - Reaped expired leases. [Reaped: " + reapCount
                            + "]");
                }
            }
            if ((reapCount == 0 && detached > 0) || detached > DETACH_LIMIT_TO_REPORT) {
                if (_logger.isInfoEnabled()) {
                    _logger.info(this.getName()
                            + " - Detached entries exist. [Detached: " + detached
                            + "]");
                }
            }

            //remove empty cell items
            reapEmptyLeaseCells();
        }

        private final void reapEmptyLeaseCells() {
            Iterator iter = _expirationList.values().iterator();
            long currentTime = getEffectiveEntryLeaseTimeForReaper(SystemTime.timeMillis());
            int numOfCellsRemoved = 0;
            int numOfCellsSkiped = 0;

            try {
                while (iter.hasNext()) {
                    Cell cell = (Cell) iter.next();
                    long cellTime = cell.getCellKey();

                    if (currentTime <= cellTime)
                        return; //finished scanning

                    if (!cell.isEmpty()) {
                        numOfCellsSkiped++;
                        continue;
                    }

                    cell.setCleaned(true);

                    //recheck, under lock to prevent a phantom cell situation
                    synchronized (cell) {
                        if (cell.isEmpty()) {
                            iter.remove();
                            numOfCellsRemoved++;
                        }
                    }

                }
            } finally {
                if (_logger.isTraceEnabled())
                    _logger.trace("Number of expired cells removed is " + numOfCellsRemoved + " number of cells skipped=" + numOfCellsSkiped);
            }
        }


        /**
         * Clean expired local transactions, every <tt>LM_EXPIRATION_TIME_INTERVAL</tt>.
         */
        private final void reapExpiredXtns() {
            if (_transactionHandler.getTimedXtns().isEmpty())
                return;

            long currentTime = SystemTime.timeMillis();
            int reapCount = 0;

            //select candidates
            Map<ServerTransaction, Long> map = _transactionHandler
                    .getTimedXtns();
            for (Map.Entry<ServerTransaction, Long> entry : map.entrySet()) {
                Long limit = entry.getValue();
                ServerTransaction tx = entry.getKey();

                //skip if already terminated or lease change
                if (tx != null && limit != null
                        && limit.longValue() < currentTime) {
                    try {
                        XtnEntry xtnEntry = _engine.getTransaction(tx);
                        boolean unused = false;
                        if (xtnEntry == null) {
                            ((ConcurrentHashMap<ServerTransaction, Long>) (_transactionHandler.getTimedXtns())).remove(tx, limit);
                            continue;
                        }
                        if (!xtnEntry.m_Active)
                            continue;  //prepare-commit lease not relevant
                        if (xtnEntry.setUnUsedIfPossible(0 /*unusedCleanTime*/, _engine.isCleanUnusedEmbeddedGlobalXtns())) {//light removal of unused xtn
                            _transactionHandler.removeUnusedTransaction(xtnEntry, true /* needLock*/);
                            unused = true;
                        }
                        //removal of candidate is done under txn table log
                        if (!unused)
                            _engine.abortSA(tx.mgr,
                                    tx,
                                    false /* from cancel-lease */,
                                    true/* verifyExpiredXtn */, false,
                                    null/* operationID */);

                        //abort was ok
                        reapCount++;

                        if (!unused && _logger.isInfoEnabled()) {
                            _logger.info("transaction [id="
                                    + tx.id
                                    + "] timed out, transaction aborted by space "
                                    + _engine.getSpaceName());
                        }
                    } catch (UnknownTransactionException ute) // abort failed-
                    // ignore this xtn
                    // (maybe aborted by
                    // user/local mngr
                    {
                        if (_logger.isDebugEnabled()) {
                            _logger.debug(
                                    this.getName()
                                            + " - transaction abort failed by space (maybe aborted by user/localTxnManager) for transaction [id="
                                            + tx.id + "]",
                                    ute);
                        }
                    } catch (Exception ex) {
                        if (_logger.isErrorEnabled()) {
                            _logger.error(
                                    this.getName()
                                            + " -  transaction abort failed by space for transaction [id="
                                            + tx.id + "]",
                                    ex);
                        }
                    }
                }
            }


            if (reapCount > 0)
                if (_logger.isDebugEnabled()) {
                    _logger.debug(this.getName()
                            + " - Reaped expired transactions. [Reaped: "
                            + reapCount
                            + ", Alive: "
                            + _transactionHandler
                            .getTimedXtns()
                            .size() + "]");
                }
        }

        private void reapExpiredIterators() {
            int reapCount = _engine.getIteratorsManager().purgeExpiredIterators();
            if (reapCount > 0)
                if (_logger.isDebugEnabled()) {
                    _logger.debug(this.getName()
                            + " - Reaped expired iterators. [Reaped: "
                            + reapCount
                            + ", Alive: "
                            + _engine.getIteratorsManager().getNumberOfActiveIterators() + "]");
                }
        }

        private final void reapPhantomGlobalXtns() {
            if (_transactionHandler.getPhantomGlobalXtns().isEmpty())
                return;

            long currentTime = SystemTime.timeMillis();
            int reapCount = 0;

            //select candidates
            Map<ServerTransaction, Long> map = _transactionHandler
                    .getPhantomGlobalXtns();
            for (Map.Entry<ServerTransaction, Long> entry : map.entrySet()) {
                Long limit = entry.getValue();
                ServerTransaction tx = entry.getKey();

                //skip if already terminated or lease change
                if (tx != null && limit != null
                        && limit.longValue() < currentTime) {
                    _transactionHandler.removeFromPhantomGlobalXtns(tx);
                    reapCount++;

                    if (_logger.isInfoEnabled()) {
                        _logger.info("Globalxtn phantom info transaction [id="
                                + tx.id
                                + "] cleared "
                                + _engine.getSpaceName());
                    }
                }
            }


            if (reapCount > 0)
                if (_logger.isDebugEnabled()) {
                    _logger.debug(this.getName()
                            + " - Phantom global transactions info. [Reaped: "
                            + reapCount
                            + ", Alive: "
                            + _transactionHandler
                            .getPhantomGlobalXtns()
                            .size() + "]");
                }
        }


        private final void reapStuck2PCPreparedXtns() {
            if (_engine.getClusterPolicy() != null && !_spaceImpl.isPrimary())
                return;  //backup receives input only from primary
            if (_transactionHandler.getPrepared2PCXtns().isEmpty())
                return;

            long currentTime = SystemTime.timeMillis();
            int reapCount = 0;

            //select candidates
            Map<ServerTransaction, Prepared2PCXtnInfo> map = _transactionHandler.getPrepared2PCXtns();
            for (Map.Entry<ServerTransaction, Prepared2PCXtnInfo> entry : map.entrySet()) {
                Prepared2PCXtnInfo pInfo = entry.getValue();
                if (!pInfo.isExpiredPrepareTime(currentTime))
                    continue;
                if (pInfo.getXtnEntry().getStatus() != XtnStatus.PREPARED)
                    continue; //irrelevant

                boolean keepOn = pInfo.extendIfPossible(_engine);
                if (keepOn)
                    continue;
                if (_logger.isWarnEnabled()) {
                    String id = pInfo.getXtnEntry().getServerTransaction().isXid() ? " xa " : "";
                    _logger.warn(this.getName()
                            + " - lease manager found stuck prepared xtn- going to abort "
                            + ", id=: "
                            + id + pInfo.getXtnEntry().getServerTransaction().id);
                }


                try {
                    _engine.abortSA(pInfo.getXtnEntry().getServerTransaction().mgr,
                            pInfo.getXtnEntry().getServerTransaction(),
                            false /* from cancel-lease */,
                            false/* verifyExpiredXtn */, false,
                            null/* operationID */);

                    if (_logger.isWarnEnabled()) {
                        String id = pInfo.getXtnEntry().getServerTransaction().isXid() ? " xa " : "";
                        _logger.warn(this.getName()
                                + " - lease manager found stuck prepared xtn- aborted "
                                + ", id=: "
                                + id + pInfo.getXtnEntry().getServerTransaction().id
                                + " [Reaped: " + reapCount + " size="
                                + _transactionHandler.getPrepared2PCXtns().size() + "]");
                    }


                } catch (UnknownTransactionException ute) {
                }// ignore


            }


            if (reapCount > 0)
                if (_logger.isWarnEnabled()) {
                    _logger.warn(this.getName()
                            + " - stuck 2PC prepared xtns. [Reaped: "
                            + reapCount
                            + ", Alive: "
                            + _transactionHandler
                            .getPrepared2PCXtns()
                            .size() + "]");
                }
        }


        /**
         * Clean unused  transactions, every <tt>LM_EXPIRATION_TIME_INTERVAL</tt>.
         */
        private final void reapUnusedXtns() {
            int unusedCleanTime = _transactionHandler
                    .getUnusedXtnCleanTime();
            int reapCount = 0;
            long currentTime = SystemTime.timeMillis();
            long expirationTime = currentTime - unusedCleanTime;
            if (_force || _lastReapedUnusedXtn <= expirationTime)
                _lastReapedUnusedXtn = currentTime; //stamp last reap time
            else {
                return; // not yet time to reap
            }


            //select candidates
            Map<ServerTransaction, XtnEntry> map = _transactionHandler
                    .getXtnTable();
            for (Map.Entry<ServerTransaction, XtnEntry> entry : map.entrySet()) {
                XtnEntry xtnEntry = entry.getValue();
                if (xtnEntry != null
                        && xtnEntry.setUnUsedIfPossible(unusedCleanTime, _engine.isCleanUnusedEmbeddedGlobalXtns())) {
                    _transactionHandler
                            .removeUnusedTransaction(xtnEntry, true /* needLock */);
                    reapCount++;
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Unused transaction [id="
                                + xtnEntry.m_Transaction.id
                                + "] cleaned,  space "
                                + _engine.getSpaceName());
                    }
                }

            }

            if (reapCount > 0)
                if (_logger.isDebugEnabled()) {
                    _logger.debug(this.getName()
                            + " - Reaped unused transactions. [Reaped: "
                            + reapCount + " ]");
                }
        }


        /**
         * Clean SpaceContent objects who's age is more than <tt>LM_EXPIRATION_TIME_PUMPSPACE_DEFAULT</tt>.
         */
        private final void reapStaleReplicas() {
            long currentTime = SystemTime.timeMillis();
            long expirationTime = currentTime
                    - _staleReplicaExpirationTime;
            if (_force || _lastReapedSpaceContentObjects < expirationTime)
                _lastReapedSpaceContentObjects = currentTime; // stamp last reap
                // time
            else
                return; // not yet time to reap

            _engine.getReplicationNode().getAdmin().clearStaleReplicas(expirationTime);
        }

        /**
         * Clean expired recent deletes table, every <tt>LM_EXPIRATION_TIME_RECENT_DELETES_DEFAULT</tt>.
         */
        private final void reapRecentDeletes() {
            if (!_cacheManager.useRecentDeletes())
                return;
            int reapCount = 0;
            try {
                long currentTime = SystemTime.timeMillis();
                long expirationTime = currentTime
                        - _expirationTimeRecentDeletes;
                long checkTime = currentTime - -LM_EXPIRATION_TIME_RECENT_DELETES_CHECK_DEFAULT;
                if (_force || _lastRepeadRecentDeletes < checkTime)
                    _lastRepeadRecentDeletes = checkTime; // stamp last reap
                    // time
                else
                    return; //not yet time to reap

                for (Iterator<RecentDeletesRepository.RecentDeleteInfo> itr = _cacheManager
                        .getRecentDeletesIterator(); itr.hasNext(); ) {
                    RecentDeletesRepository.RecentDeleteInfo rdinfo = itr.next();
                    long etime = rdinfo.getTimeBase();
                    if (etime < expirationTime || (etime == Long.MAX_VALUE && _cacheManager.requiresEvictionReplicationProtection())) {
                        //lock entry for pinning check
                        ILockObject entryLock = null;
                        try {
                            entryLock = _cacheManager
                                    .getLockManager()
                                    .getLockObject(rdinfo.getUid());
                            synchronized (entryLock) {
                                RecentDeletesRepository.RecentDeleteInfo curInfo = _cacheManager.getRecentDeleteInfo(rdinfo.getUid());
                                if (curInfo != rdinfo)
                                    continue; //not the same

                                IEntryHolder entry = _cacheManager
                                        .getEntryByUidFromPureCache(rdinfo.getUid());
                                if (entry == null)
                                    throw new RuntimeException("RecentDeletes reaper: entry not in memory "
                                            + rdinfo.getUid());
                                if (!entry.isDeleted())
                                    throw new RuntimeException("RecentDeletes reaper: entry not deleted "
                                            + rdinfo.getUid());

                                if (curInfo.getTimeBase() == Long.MAX_VALUE) {//markers repository. if marker reached- set the time to current time
                                    //if xtn is not terminated yet- recheck later
                                    XtnEntry xtnEntry = rdinfo.getXtn() != null ? _engine.getTransaction(rdinfo.getXtn()) : null;
                                    if (xtnEntry != null && xtnEntry.getStatus() != XtnStatus.COMMITED && xtnEntry.getStatus() != XtnStatus.ROLLED)
                                        continue; //xtn still on- dont check markers repository yet
                                    if (_cacheManager.getEvictionReplicationsMarkersRepository().isEntryEvictable(rdinfo.getUid(), false /*alreadyLocked = false*/)) {//entry reached its destination, now its a regular recent-delete
                                        _cacheManager.insertToRecentDeletes(entry, SystemTime.timeMillis(), rdinfo.getXtn());
                                    }
                                    continue;
                                }
                                //remove current info from recentDeletes + remove the entry itself

                                itr.remove();

                                reapCount++;
                                _cacheManager
                                        .removeEntryFromCache(entry,
                                                false /* initiatedByEvictionStrategy */,
                                                true /* locked */,
                                                null,
                                                RecentDeleteCodes.REMOVE_DUMMY);

                            }
                        } finally {
                            if (entryLock != null)
                                _cacheManager
                                        .getLockManager()
                                        .freeLockObject(entryLock);
                        }
                    }
                }
            } catch (Exception ex) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(
                            this.getName()
                                    + " - caught exception while reaping recent deleted entries.",
                            ex);
                }
            }

            if (reapCount > 0) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(this.getName()
                            + " - Reaped content of recently deleted entries. [Reaped: "
                            + reapCount
                            + ", Remaining:"
                            + _cacheManager
                            .getNumOfRecentDeletes()
                            + " ]");
                }
                if (_logger.isTraceEnabled() && _cacheManager.getNumOfRecentDeletes() == 0)
                {
                    _logger.trace(this.getName()
                            + " - Number of entries in cache after reap all recent deletes:"
                            + _cacheManager.getEnriesSize());
                }
            }
        }


        /**
         * Clean expired recent updates table, every <tt>LM_EXPIRATION_TIME_RECENT_UPDATES_DEFAULT</tt>.
         */
        private final void reapRecentUpdates() {
            if (!_cacheManager.useRecentUpdatesForPinning())
                return;
            int reapCount = 0;
            Context context = null;

            try {
                long currentTime = SystemTime.timeMillis();
                long expirationTime = currentTime
                        - _expirationTimeRecentUpdates;
                long checkTime = currentTime - -LM_EXPIRATION_TIME_RECENT_UPDATES_CHECK_DEFAULT;
                if (_force || _lastReapedRecentUpdates < checkTime)
                    _lastReapedRecentUpdates = checkTime; // stamp last reap
                    // time
                else
                    return; // not yet time to reap

                context = _cacheManager.getCacheContext();

                for (Iterator<RecentUpdatesRepository.RecentUpdateInfo> itr = _cacheManager
                        .getRecentUpdatesIterator(); itr.hasNext(); ) {
                    RecentUpdatesRepository.RecentUpdateInfo rdinfo = itr.next();
                    long etime = rdinfo.getTimeBase();
                    if (etime < expirationTime || (etime == Long.MAX_VALUE && _cacheManager.requiresEvictionReplicationProtection())) {
                        //lock entry for pinning check
                        ILockObject entryLock = null;
                        try {
                            entryLock = _cacheManager
                                    .getLockManager()
                                    .getLockObject(rdinfo.getUid());
                            synchronized (entryLock) {
                                RecentUpdatesRepository.RecentUpdateInfo curInfo = _cacheManager.getRecentUpdateInfo(rdinfo.getUid());
                                if (curInfo != rdinfo)
                                    continue; //not the same

                                IEntryHolder entry = _cacheManager
                                        .getEntryByUidFromPureCache(rdinfo.getUid());
                                if (entry == null)
                                    throw new RuntimeException("RecentUpdates reaper: entry not in memory "
                                            + rdinfo.getUid());

                                if (curInfo.getTimeBase() == Long.MAX_VALUE) {//markers repository. if marker reached- set the time to current time
                                    //if xtn is not terminated yet- recheck later
                                    XtnEntry xtnEntry = rdinfo.getXtn() != null ? _engine.getTransaction(rdinfo.getXtn()) : null;
                                    if (xtnEntry != null && xtnEntry.getStatus() != XtnStatus.COMMITED && xtnEntry.getStatus() != XtnStatus.ROLLED)
                                        continue; //xtn still on- dont check markers repository yet
                                    if (_cacheManager.getEvictionReplicationsMarkersRepository().isEntryEvictable(rdinfo.getUid(), false /*alreadyLocked = false*/)) {//entry reached its destination, now its a regular recent-delete
                                        _cacheManager.insertToRecentUpdates(entry, SystemTime.timeMillis(), rdinfo.getXtn());
                                    }
                                    continue;
                                }


                                itr.remove();

                                reapCount++;
                                _cacheManager.unpinIfNeeded(context, entry,
                                        null /* template */,
                                        null /* pEntry */);
                            }
                        } finally {
                            if (entryLock != null)
                                _cacheManager
                                        .getLockManager()
                                        .freeLockObject(entryLock);
                        }
                    }
                }
            } catch (Exception ex) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(
                            this.getName()
                                    + " - caught exception while reaping recent updated entries content.",
                            ex);
                }
            } finally {
                if (context != null)
                    context = _cacheManager.freeCacheContext(context);
            }

            if (reapCount > 0 && _logger.isDebugEnabled()) {
                _logger.debug(this.getName()
                        + " - Reaped expired content of recently updated entries. [Reaped: "
                        + reapCount + ", Remaining:"
                        + _cacheManager.getNumOfRecentUpdates()
                        + "]");
            }
        }


        private final void reapFifoXtnsEntryInfo() {
            Context context = null;
            int reapCount = 0;
            try {
                long currentTime = SystemTime.timeMillis();
                long expirationTime = currentTime
                        - LM_EXPIRATION_TIME_FIFOENTRY_XTNINFO;

                ConcurrentHashMap<FifoXtnEntryInfo, FifoXtnEntryInfo> terminatingXtnsEntries = _cacheManager
                        .getTerminatingXtnsEntries();
                if (terminatingXtnsEntries.isEmpty())
                    return;

                for (Iterator<FifoXtnEntryInfo> itr = terminatingXtnsEntries.values()
                        .iterator(); itr.hasNext(); ) {
                    FifoXtnEntryInfo fxe = itr.next();
                    if (fxe.getChangeTime() >= expirationTime)
                        continue;

                    //lock entry & check again
                    if (context == null)
                        context = _cacheManager.getCacheContext();

                    IEntryHolder eh = null;
                    if (!_cacheManager.isEvictableCachePolicy()) {
                        eh = _cacheManager
                                .getEntryByUidFromPureCache(fxe.getUid());
                        if (eh == null) // deleted entry- delete from hash
                        {
                            itr.remove();
                            reapCount++;
                            continue;
                        }
                    }

                    ILockObject entryLock = null;
                    try {
                        entryLock = eh != null ? _cacheManager
                                .getLockManager()
                                .getLockObject(eh) : _cacheManager
                                .getLockManager()
                                .getLockObject(fxe.getUid());
                        synchronized (entryLock) {
                            if (fxe.getChangeTime() < expirationTime) {
                                itr.remove();
                                reapCount++;
                            }
                        }//synchronized(entryLock)
                    } finally {
                        if (entryLock != null) {
                            _cacheManager
                                    .getLockManager()
                                    .freeLockObject(entryLock);
                            entryLock = null;
                        }
                    }

                }//for

            } catch (Exception ex) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(
                            this.getName()
                                    + " - caught exception while reaping transaction content for FIFO entries.",
                            ex);
                }
            } finally {
                context = _cacheManager.freeCacheContext(context);
            }

            if (reapCount > 0)
                if (_logger.isDebugEnabled()) {
                    _logger.debug(this.getName()
                            + " - Reaped expired transaction content of FIFO entries. [Reaped: "
                            + reapCount
                            + ", Pending: "
                            + _cacheManager
                            .getTerminatingXtnsEntries()
                            .size() + "]");
                }
        }


        /**
         * Clean reached markers if markers repository is activated
         */
        private final void reapReachedMarkers() {
            if (!_cacheManager.requiresEvictionReplicationProtection())
                return;
            int reapCount = 0;

            try {
                long currentTime = SystemTime.timeMillis();
                long expirationTime = currentTime
                        - LM_CHECK_TIME_MARKERS_REPOSITORY_DEFAULT;
                if (_force || _lastReapedMarkersRepository < expirationTime)
                    _lastReapedMarkersRepository = currentTime; // stamp last reap
                    // time
                else
                    return; // not yet time to reap

                reapCount = _cacheManager.getEvictionReplicationsMarkersRepository().reapUnused();


            } catch (Exception ex) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(
                            this.getName()
                                    + " - caught exception while reaping reached markers ",
                            ex);
                }
            }

            if (reapCount > 0 && _logger.isDebugEnabled()) {
                _logger.debug(this.getName()
                        + " - Reaped reached markers. [Reaped: "
                        + reapCount + ", Remaining:"
                        + _cacheManager.getEvictionReplicationsMarkersRepository().size()
                        + "]");
            }
            if (_logger.isTraceEnabled() && _cacheManager.getEvictionReplicationsMarkersRepository().size() == 0)
            {
                _logger.trace(this.getName()
                        + " - Number of entries in cache after reap all recent deletes:"
                        + _cacheManager.getEnriesSize());
            }
        }

        private void reapRecentExtendedUpdates() {
            int reaped = 0;
            if (!isSupportsRecentExtendedUpdates())
                return;
            try {

                ITemplatePacket templatePacket = new TemplatePacket();
                templatePacket.setFieldsValues(new Object[0]);
                IServerTypeDesc typeDesc = _cacheManager.getTypeManager()
                        .loadServerTypeDesc(templatePacket);

                //for each type scan its indexes and call
                final IServerTypeDesc[] subTypes = typeDesc.getAssignableTypes();
                for (IServerTypeDesc subtype : subTypes) {
                    TypeData td = _cacheManager.getTypeData(subtype);
                    if (td == null)
                        continue;
                    if (!td.hasIndexes())
                        continue;
                    final TypeDataIndex[] indexes = td.getIndexes();
                    for (TypeDataIndex index : indexes) {
                        if (!index.isExtendedIndex())
                            continue;
                        reaped += index.getExtendedIndex().reapExpired();
                        if (index.getExtendedFGIndex() != null)
                            reaped += index.getExtendedFGIndex().reapExpired();
                    }
                }
            } catch (Exception ex) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(
                            this.getName()
                                    + " - caught exception while reaping reached markers ",
                            ex);
                }
            }

            if (reaped > 0 && _logger.isDebugEnabled()) {
                _logger.debug(this.getName()
                        + " - Reaped reapRecentExtendedUpdates. [Reaped: "
                        + reaped + "]");
            }


        }


    } //LeaseReaper class

    /**
     * Cell grouping entry/template within the <tt>LM_EXPIRATION_TIME_INTERVAL</tt> boundary of
     * their lease expiration.
     */
    private static final class Cell {
        //cell key in cells' list
        private final Long _expirationTime;
        //true if cell is detached
        private volatile boolean _cleaned;
        //uids of entries expired here. for off-heap its uid, otherwize entryholder
        private final IStoredList<Object> _entriesExpired;
        //uids of notify templates expired here. since notify templates are
        //relativly rare , we create it lazily
        private volatile IStoredList<Object> _notifyTemplatesExpired;

        private Cell(int segmentsPerExpirationCell, Long expirationTime) {
            _expirationTime = expirationTime;
            if (segmentsPerExpirationCell == 1)
                _entriesExpired = StoredListFactory.createConcurrentList(true);
            else
                _entriesExpired = StoredListFactory.createConcurrentSegmentedList(true /*supportsFifoPerSegment*/,segmentsPerExpirationCell,false /* padded*/);
        }

        private Long getCellKey() {
            return _expirationTime;
        }

        private boolean isCleaned() {
            return _cleaned;
        }

        private void setCleaned(boolean val) {
            _cleaned = val;
        }

        private boolean isEmpty() {
            return _entriesExpired.isEmpty() && (_notifyTemplatesExpired == null || _notifyTemplatesExpired.isEmpty());
        }

        private Iterator<IEntryHolder> mateExpriedEntriesUidsIter(SpaceEngine engine) {
            return new EntriesCellIter(_entriesExpired, engine);
        }

        private Iterator<IEntryHolder> mateExpriedNotifyTemplatesUidsIter() {
            IStoredList<Object> notifyTemplates = _notifyTemplatesExpired;
            return notifyTemplates != null ? new TemplatesCellIter(notifyTemplates) : null;
        }

        private void register(ILeasedEntryCacheInfo leaseCacheInfo, IEntryHolder entry, int objectType) {
            if (objectType == ObjectTypes.ENTRY) {
                IObjectInfo<Object> pos = entry.isBlobStoreEntry() ? _entriesExpired.add(((IBlobStoreEntryHolder) entry).getBlobStoreResidentPart().getUID()) : _entriesExpired.add(entry);
                leaseCacheInfo.setLeaseManagerListRefAndPosition(_entriesExpired, pos);
            } else {
                IStoredList<Object> notifyTemplatesExpired = _notifyTemplatesExpired;
                if (notifyTemplatesExpired == null) {
                    synchronized (this) {
                        if (_notifyTemplatesExpired == null)
                            _notifyTemplatesExpired = StoredListFactory.createConcurrentList(true /*supportFifoPerSegment*/);
                        notifyTemplatesExpired = _notifyTemplatesExpired;
                    }//synchronized
                }
                IObjectInfo<Object> pos = notifyTemplatesExpired.add(entry);
                leaseCacheInfo.setLeaseManagerListRefAndPosition(notifyTemplatesExpired, pos);
            }
        }

        private void unregisterByPos(IObjectInfo<Object> pos, boolean isEntry) {
            if (isEntry)
                _entriesExpired.remove(pos);
            else throw new UnsupportedOperationException();
        }

        private static final class EntriesCellIter implements Iterator<IEntryHolder> {
            private IStoredListIterator<Object> _pos;
            private final IStoredList<Object> _entriesExp;
            private final SpaceEngine _engine;

            private EntriesCellIter(IStoredList<Object> entriesExpired, SpaceEngine engine) {
                _engine = engine;
                _entriesExp = entriesExpired;
                _pos = _entriesExp.establishListScan(true /*random scan*/);
            }

            public void remove() {
            }

            public boolean hasNext() {
                return _pos != null;
            }

            public IEntryHolder next() {
                IEntryHolder sl_res = null;
                Object val = _pos.getSubject();
                if (_engine.getCacheManager().isblobStoreDataSpace() && val != null && (val instanceof String)) {
                    sl_res = _engine.getCacheManager().getEntryByUidFromPureCache((String) val);
                } else
                    sl_res = (IEntryHolder) val;
                _pos = _entriesExp.next(_pos);
                return sl_res;

            }
        }

        private static final class TemplatesCellIter implements Iterator<IEntryHolder> {
            private IStoredListIterator<Object> _pos;
            private final IStoredList<Object> _templatesExp;

            private TemplatesCellIter(IStoredList<Object> templatesExpired) {
                _templatesExp = templatesExpired;
                _pos = _templatesExp.establishListScan(false /*random scan*/);
            }

            public void remove() {
            }

            public boolean hasNext() {
                return _pos != null;
            }

            public IEntryHolder next() {
                IEntryHolder sl_res = (IEntryHolder) _pos.getSubject();
                _pos = _templatesExp.next(_pos);
                return sl_res;

            }
        }

    }


}
