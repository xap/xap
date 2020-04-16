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

package com.j_spaces.core;

import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.cache.AbstractCacheManager;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.blobStore.memory_pool.AbstractMemoryPool;
import com.j_spaces.core.cache.blobStore.memory_pool.OffHeapMemoryPool;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.SystemProperties;

import java.io.Closeable;
import java.lang.management.ManagementFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_EXPLICIT_GC_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_EXPLICIT_GC_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_EXPLICIT_LEASE_REAPER_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_EXPLICIT_LEASE_REAPER_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_ENABLED_PRIMARY_ONLY;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_ENABLED_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_RETRY_COUNT_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_RETRY_COUNT_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_RETRY_YIELD_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_RETRY_YIELD_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_SYNCHRONUS_EVICTION_WATERMARK_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_SYNCHRONUS_EVICTION_WATERMARK_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_MEMORY_USAGE_ENABLED_PROP;

@com.gigaspaces.api.InternalApi
public class MemoryManager implements Closeable {

    /**
     * true - perform memory monitor only for write-type operations, false for all.
     */
    private volatile boolean _monitorOnlyWriteOps;

    private final String _spaceName;
    private final String _containerName;
    private final AbstractCacheManager _cacheManager;
    private final LeaseManager _leaseManager;

    //---------------  memory usage monitoring -----------------
    /**
     * if true- memory usage is monitored before each object-creating API.
     */
    final private boolean _enabled;
    final private boolean _restartOnFailover;
    final private float _memoryUsageHighLevel; // Percentage to allow up to
    final private float _memoryUsageSyncEvictionLevel; // from this level evict is done in synchronious manner. default is _memoryUsageHighLevel + (max - _memoryUsageHighLevel)/2
    final private float _memoryWriteOnlyBlock;    // Y% lower than _memoryUsageHighLevel
    final private float _memoryWriteOnlyCheck;    // X% lower than _memoryUsageHighLevel, where X >= Y
    final private float _memoryUsageLowLevel;  // Percentage to allow down to
    final private int _memoryRetryCount;        // number of retries to lower memory level below _memoryWriteOnlyBlock
    final private long _layoffTimeout;    // in milliseconds
    final private int _evictionQuota; //in case LRU we evict unused entries in order to ensure space
    final private Evictor _evictor;
    final private boolean _callGC;
    final private boolean _gcBeforeShortage;
    final private boolean _forceLeaseReaper;

    private final IProcessMemoryManager _processMemoryManager;
    private final AbstractMemoryPool _offHeapStorage;

    private final Logger _logger;

    public MemoryManager(String spaceName, String containerName, AbstractCacheManager cacheManager, LeaseManager leaseManager, boolean isPrimary) {
        _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_MEMORYMANAGER +
                "." + spaceName + "." + SpaceImpl.extractInstanceIdFromContainerName(containerName));
        _spaceName = spaceName;
        _containerName = containerName;
        _cacheManager = cacheManager;
        _leaseManager = leaseManager;
        _processMemoryManager = ProcessMemoryManager.INSTANCE;

        final String fullSpaceName = JSpaceUtilities.createFullSpaceName(containerName, spaceName);
        final SpaceConfigReader configReader = new SpaceConfigReader(fullSpaceName);
        //do we have to monitor memory usage ?
        String monitorMemoryUsage = configReader.getSpaceProperty(ENGINE_MEMORY_USAGE_ENABLED_PROP, ENGINE_MEMORY_USAGE_ENABLED_DEFAULT);
        startHeapDumpMBean();
        _enabled = isEnabled(monitorMemoryUsage, isPrimary);
        _restartOnFailover = monitorMemoryUsage.equalsIgnoreCase(ENGINE_MEMORY_USAGE_ENABLED_PRIMARY_ONLY);
        _layoffTimeout = configReader.getLongSpaceProperty(
                ENGINE_MEMORY_USAGE_RETRY_YIELD_PROP, ENGINE_MEMORY_USAGE_RETRY_YIELD_DEFAULT);

        _offHeapStorage = initOffHeapStorage();

        if (_enabled) {
            _evictor = new Evictor();

            try {
                _memoryUsageHighLevel = configReader.getFloatSpaceProperty(
                        ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_PROP, ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_DEFAULT);
                _memoryWriteOnlyBlock = configReader.getFloatSpaceProperty(
                        ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_PROP, ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_DEFAULT);
                _memoryWriteOnlyCheck = configReader.getFloatSpaceProperty(
                        ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_PROP, ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_DEFAULT);
                _forceLeaseReaper = configReader.getBooleanSpaceProperty(
                        ENGINE_MEMORY_EXPLICIT_LEASE_REAPER_PROP, ENGINE_MEMORY_EXPLICIT_LEASE_REAPER_DEFAULT);

            } catch (Exception ex) {
                throw new RuntimeException("Invalid high percentage ratio value for memory usage limit: " + ex.toString());
            }

            try {
                _memoryUsageLowLevel = configReader.getFloatSpaceProperty(
                        ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_PROP, ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_DEFAULT);
            } catch (Exception ex) {
                throw new RuntimeException("Invalid low percentage ratio value for memory usage limit: " + ex.toString(), ex);
            }
            float syncEvictLevel;
            try {
                syncEvictLevel = configReader.getFloatSpaceProperty(
                        ENGINE_MEMORY_USAGE_SYNCHRONUS_EVICTION_WATERMARK_PROP, ENGINE_MEMORY_USAGE_SYNCHRONUS_EVICTION_WATERMARK_DEFAULT);
            } catch (Exception ex) {
                throw new RuntimeException("Invalid sync-eviction percentage ratio value for memory usage limit: " + ex.toString(), ex);
            }
            if (syncEvictLevel == 0)
                _memoryUsageSyncEvictionLevel = _memoryUsageHighLevel + (100 - _memoryUsageHighLevel) / 2;
            else
                _memoryUsageSyncEvictionLevel = Math.max(syncEvictLevel, _memoryUsageHighLevel);

            //eviction quota
            try {
                _evictionQuota = configReader.getIntSpaceProperty(
                        ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_PROP, ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_DEFAULT);
                cacheManager.setEvictionQuota(_evictionQuota);
            } catch (Exception ex) {
                throw new RuntimeException("Invalid value for eviction-quota: " + ex.toString(), ex);
            }

            try {
                _memoryRetryCount = configReader.getIntSpaceProperty(
                        ENGINE_MEMORY_USAGE_RETRY_COUNT_PROP, ENGINE_MEMORY_USAGE_RETRY_COUNT_DEFAULT);
            } catch (Exception ex) {
                throw new RuntimeException("Invalid value for Memory Retry Count: " + ex.toString(), ex);
            }

            try {
                _callGC = configReader.getBooleanSpaceProperty(
                        ENGINE_MEMORY_EXPLICIT_GC_PROP, ENGINE_MEMORY_EXPLICIT_GC_DEFAULT);
            } catch (Exception ex) {
                throw new RuntimeException("Invalid value for Memory explicit GC: " + ex.toString(), ex);
            }

            try {
                _gcBeforeShortage = configReader.getBooleanSpaceProperty(
                        ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_PROP, ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_DEFAULT);
            } catch (Exception ex) {
                throw new RuntimeException("Invalid value for Memory explicit GC: " + ex.toString(), ex);
            }

            // The level picture should look like this:
            // _memoryUsageHighLevel >= _memoryWriteOnlyBlock >= _memoryWriteOnlyCheck >= _memoryUsageLowLevel
            if (_memoryUsageLowLevel > _memoryUsageHighLevel ||
                    _memoryUsageHighLevel < _memoryUsageLowLevel ||
                    _memoryWriteOnlyBlock < _memoryWriteOnlyCheck ||
                    _memoryUsageHighLevel < _memoryWriteOnlyBlock ||
                    _memoryWriteOnlyCheck < _memoryUsageLowLevel ||
                    _memoryUsageHighLevel > 100 ||
                    _memoryUsageLowLevel < 0)
                throw new RuntimeException("Invalid percentage ratio definition for Memory Management.\n"
                        + "\tShould obey: High Watermark Percentage >= Write only block percentage >= Write only check percentage >= Low Watermark Percentage \n"
                        + "\tPlease refer to the space xml file under <memory_usage> tag.\n"
                        + "\tExtracted parameters: (In the absence of either parameter, the default is used)"
                        + "\n\t\tHigh Watermark Percentage: " + _memoryUsageHighLevel
                        + ",\n\t\tWrite only block percentage: " + _memoryWriteOnlyBlock
                        + ",\n\t\tWrite only check percentage: " + _memoryWriteOnlyCheck
                        + ",\n\t\tLow Watermark Percentage: " + _memoryUsageLowLevel);

        }//if (_monitorMemoryUsage)
        else // no memory monitor
        {
            _memoryUsageHighLevel = 100;
            _memoryWriteOnlyBlock = 100;
            _memoryWriteOnlyCheck = 100;
            _memoryUsageLowLevel = 100;
            _memoryRetryCount = 0;
            _evictionQuota = 0;
            _evictor = null;
            _callGC = false;
            _gcBeforeShortage = false;
            _forceLeaseReaper = false;
            _memoryUsageSyncEvictionLevel = 100;
        }

        if (_logger.isDebugEnabled()) {
            if (_enabled) {
                _logger.debug("Memory manager is enabled [" +
                        "high_watermark=" + _memoryUsageHighLevel + "%, " +
                        "low_watermark=" + _memoryUsageLowLevel + "%, " +
                        "write_only_block=" + _memoryWriteOnlyBlock + "%, " +
                        "write_only_check=" + _memoryWriteOnlyCheck + "%, " +
                        "retry_count=" + _memoryRetryCount + "]");
            } else if (_restartOnFailover)
                _logger.debug("Memory manager is disabled, but will be activated if this instance becomes primary");
            else
                _logger.debug("Memory manager is disabled");
        }

        start();
    }

    private AbstractMemoryPool initOffHeapStorage() {
        if (!(_cacheManager instanceof CacheManager))
            return null;
        final CacheManager cacheManager = (CacheManager)_cacheManager;
        if (!cacheManager.isBlobStoreCachePolicy())
            return null;
        if (cacheManager.hasBlobStoreOffHeapCache())
            return cacheManager.getBlobStoreStorageHandler().getOffHeapCache();
        if (cacheManager.getBlobStoreStorageHandler().getOffHeapStore() != null)
            return cacheManager.getBlobStoreStorageHandler().getOffHeapStore();
        return null;
    }

    private synchronized void startHeapDumpMBean() {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("org.xap:type=HeapDumpMBean");
            if(!mbs.isRegistered(name)) {
                HeapDump heapDump = new HeapDump();
                heapDump.setEnabled(Boolean.getBoolean(SystemProperties.CREATE_HEAP_DUMP_ON_MEMORY_SHORTAGE));
                heapDump.setMaxHeaps(Integer.getInteger(SystemProperties.MAX_HEAPS_ON_MEMORY_SHORTAGE, 1));
                heapDump.setQuietPeriod(Long.getLong(SystemProperties.HEAPS_ON_MEMORY_SHORTAGE_QUIET_PERIOD, 1));
                mbs.registerMBean(heapDump, name);
                _logger.debug("Register HeapDumpMBean " + name);
            }
        }catch(Exception e){
            _logger.warn("failed to start HeapDumpMBean" +  e);
        }
    }

    private void createDumpIfAppropriate(){
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("org.xap:type=HeapDumpMBean");
            boolean created = (Boolean)mbs.invoke(name, "onMemoryShortage",  null, null);
            if(created){
                _logger.debug("Heap dump created");
            }else{
                _logger.trace("Heap dump not created");
            }
        }catch(Exception e){
            _logger.warn("failed to start HeapDumpMBean" +  e);
        }
    }

    public boolean isEnabled() {
        return _enabled;
    }

    public boolean isRestartOnFailover() {
        return _restartOnFailover;
    }

    private boolean isEnabled(String value, boolean isPrimary) {
        if (value.equalsIgnoreCase("true"))
            return true;
        if (value.equalsIgnoreCase("false"))
            return false;
        if (value.equalsIgnoreCase(ENGINE_MEMORY_USAGE_ENABLED_PRIMARY_ONLY))
            return isPrimary;
        throw new IllegalArgumentException("Unsupported value for " + FULL_ENGINE_MEMORY_USAGE_ENABLED_PROP + ": " + value);
    }

    /**
     * return the eviction quota as defined
     *
     * @return eviction quota
     */
    private int getEvictionBatchSize() {
        return _evictionQuota;
    }

    private void setMonitorOnlyWriteOps(boolean val) {
        _monitorOnlyWriteOps = val;
    }

    /**
     * check if we reached the memory-usage "watermark". if memory usage is not enabled do nothing.
     *
     * @param isWriteTypeOperation is write operation
     */
    public void monitorMemoryUsage(boolean isWriteTypeOperation) throws MemoryShortageException {
        MemoryEvictionDecision res;

        if (isWriteTypeOperation) {
            // TODO: Skip off-heap check if type is not blob-store-enabled
            monitorOffHeap();
        }
        if (_enabled && ((res = monitorMemoryUsageWithNoEviction_Impl(isWriteTypeOperation)) != MemoryEvictionDecision.NO_EVICTION)) {
            // starts the eviction thread
            _evictor.evict(isWriteTypeOperation, res == MemoryEvictionDecision.SYNC_EVICTION);
        }
    }

    /**
     * Perform all memory check but do not perform eviction, instead returns true if eviction should
     * be called this method will be used when loading cache in warm start with LRU mode in this
     * case if the memory limit reached loading of entries from the persistence storage should be
     * stopped instead of perform eviction and continue the cache loading.
     *
     * @param isWriteTypeOperation true iff the space operation that triggered this check is write or
     *                             equivalent.
     * @return true iff eviction should be called
     */

    private void monitorOffHeap() {
        if (_offHeapStorage == null)
            return;

        long bytesUsed = _offHeapStorage.getUsedBytes();
        long threshold = _offHeapStorage.getThreshold();
        int MBFactor = 1024 * 1024;

        if (bytesUsed >= threshold * (_memoryUsageHighLevel / 100)) {
            String used = bytesUsed > MBFactor ?  (bytesUsed / MBFactor) + " mb" : bytesUsed + " b" ;
            String max = threshold > MBFactor ? (threshold / MBFactor) +" mb" : threshold + " b";
            String hostId = SystemInfo.singleton().network().getHostId();
            String msg = "Off Heap Memory reached "+_memoryUsageHighLevel+"% at: host: " + hostId + ", space " + _spaceName
                    + ", container " + _containerName + ", total off heap memory: " + max + ", used off heap memory: " + used;
            throw new OffHeapMemoryShortageException(msg, _spaceName, _containerName, hostId, bytesUsed, threshold);
        }
    }

    public boolean monitorMemoryUsageWithNoEviction(boolean isWriteTypeOperation) {
        MemoryEvictionDecision res = monitorMemoryUsageWithNoEviction_Impl(isWriteTypeOperation);
        return res != MemoryEvictionDecision.NO_EVICTION;
    }

    /**
     * EVICTION DECISIONS.
     */
    private enum MemoryEvictionDecision {
        NO_EVICTION, ASYNC_EVICTION, SYNC_EVICTION
    }

    private MemoryEvictionDecision monitorMemoryUsageWithNoEviction_Impl(boolean isWriteTypeOperation) {
        // perform memory monitor only for write-type operations
        if (_monitorOnlyWriteOps && !isWriteTypeOperation && !_cacheManager.isBlobStoreCachePolicy())
            return MemoryEvictionDecision.NO_EVICTION;

        double rate = getMemoryUsageRate(false, !_cacheManager.isEvictableCachePolicy());

        // set _monitorOnlyWriteOps flag for next call:
        // Monitor only write-type ops' if the prev' level was LE _memoryWriteOnlyCheck
        boolean monitorOnlyWriteOps = rate <= _memoryWriteOnlyCheck;
        if (monitorOnlyWriteOps != _monitorOnlyWriteOps) {
            if (_processMemoryManager.isAsyncCheckEnabled() && _logger.isInfoEnabled()) {
                if (monitorOnlyWriteOps) { // below
                    _logger.info("Current memory usage rate [" + rate + "] is below write_only_check_percentage threshold [" + _memoryWriteOnlyCheck + "], moving to async measurement of memory usage rate");
                } else {
                    _logger.info("Current memory usage rate [" + rate + "] is above write_only_check_percentage threshold [" + _memoryWriteOnlyCheck + "], moving to sync measurement of memory usage rate");
                }
            }
            _monitorOnlyWriteOps = monitorOnlyWriteOps;
        }

        if (!monitorOnlyWriteOps) {
            rate = getMemoryUsageRate(false, false);
        }

        // if level is under the _memoryWriteOnlyBlock threshold, allow all operations
        if (rate < _memoryWriteOnlyBlock)
            return MemoryEvictionDecision.NO_EVICTION;

        // if level is between _memoryUsageHighLevel and _memoryWriteOnlyBlock then
        // allow only non-write operations.
        if (rate < _memoryUsageHighLevel && !isWriteTypeOperation)
            return MemoryEvictionDecision.NO_EVICTION;

        if (_cacheManager.isResidentEntriesCachePolicy()) {
            if (_gcBeforeShortage)
                rate = getMemoryUsageRate(true, false);
            MemoryShortageException ex = shortageCheck(rate, isWriteTypeOperation);
            if (ex != null) {
                createDumpIfAppropriate();
                throw ex;
            }
            return MemoryEvictionDecision.NO_EVICTION;
        }
        return rate < _memoryUsageSyncEvictionLevel ? MemoryEvictionDecision.ASYNC_EVICTION : MemoryEvictionDecision.SYNC_EVICTION;
    }

    @Override
    public void close() {
        if (_evictor != null)
            _evictor.stop();
    }

    private void start() {
        if (_evictor != null && _cacheManager.isEvictableCachePolicy())
            _evictor.start();
    }

    /**
     * get JVM rate.
     */
    private double getMemoryUsageRate(boolean forceGC, boolean asyncCheckIfEnabled) {
        if (forceGC) {
            if (_forceLeaseReaper && _leaseManager != null) {
                if (_logger.isDebugEnabled())
                    _logger.debug("forcing lease reaper cycle");
                try {
                    _leaseManager.forceLeaseReaperCycle(false);
                } catch (InterruptedException e) {
                    //break waiting, restore interrupted state
                    Thread.currentThread().interrupt();
                }
            }
            if (_logger.isDebugEnabled())
                _logger.debug("explicit gc call");
            _processMemoryManager.performGC();
        }

        return _processMemoryManager.getMemoryUsagePercentage(asyncCheckIfEnabled);
    }

    /**
     * Checks the memory usage, and creates an exception if find a shortage.
     *
     * @param rate           memory usage rate
     * @param writeOperation is write operation
     * @return exception if a shortage was found
     */
    private MemoryShortageException shortageCheck(double rate, boolean writeOperation) {
        // if no more retries and still the rate is greater than _memoryUsageHighLevel OR
        // it is between _memoryWriteOnlyBlock and _memoryUsageHighLevel for a write-type
        // operation, then throw a MemoryShortageException
        if (shouldBlock(rate, writeOperation)) {
            if (_gcBeforeShortage)
                rate = getMemoryUsageRate(true, false); // recheck rate and force GC
            if (shouldBlock(rate, writeOperation)) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Memory shortage in cache: " + _spaceName);
                }

                long usage = (long) ((rate * _processMemoryManager.getMaximumMemory()) / 100.0); // convert rate to usage from % to bytes
                return new MemoryShortageException(_spaceName, _containerName, SystemInfo.singleton().network().getHostId(), usage, _processMemoryManager.getMaximumMemory());
            }
        }
        return null;
    }

    //checks for both read %% write types
    private MemoryShortageException[] shortageChecks(double rate) {
        // if no more retries and still the rate is greater than _memoryUsageHighLevel OR
        // it is between _memoryWriteOnlyBlock and _memoryUsageHighLevel for a write-type
        // operation, then throw a MemoryShortageException
        if (shouldBlock(rate, true) || shouldBlock(rate, false)) {
            if (_gcBeforeShortage)
                rate = getMemoryUsageRate(true, false); // re check rate and force GC
            if (shouldBlock(rate, true) || shouldBlock(rate, false)) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Memory shortage in cache: " + _spaceName);
                }

                long usage = (long) ((rate * _processMemoryManager.getMaximumMemory()) / 100.0); // convert rate to usage from % to bytes

                @SuppressWarnings("ThrowableInstanceNeverThrown") MemoryShortageException ex = new MemoryShortageException(_spaceName, _containerName, SystemInfo.singleton().network().getHostId(), usage, _processMemoryManager.getMaximumMemory());
                MemoryShortageException[] res = new MemoryShortageException[2];
                if (shouldBlock(rate, true))
                    res[0] = ex;
                if (shouldBlock(rate, false))
                    res[1] = ex;
                return res;
            }
        }
        return null;
    }


    private boolean shouldBlock(double rate, boolean writeOperation) {
        return rate > _memoryUsageHighLevel || (rate >= _memoryWriteOnlyBlock && writeOperation);
    }

    /**
     * Evicts entries from the cache according to the limits.
     *
     * @author Guy Korland
     */
    private class Evictor {
        private boolean _readTypeEvict = false;
        private boolean _writeTypeEvict = false;
        volatile private MemoryShortageException _writeTypeMemoryException = null;
        volatile private MemoryShortageException _readTypeMemoryException = null;
        private EvictorRunner _runner;

        // Used to synchronize between the runner and new eviction call
        final private Object _lock = new Object();

        private void evict(boolean aWriteTypeOperation, boolean synchronousCall)
                throws MemoryShortageException {
            if (_logger.isDebugEnabled())
                _logger.debug("Call evict on operation: " + aWriteTypeOperation + " synchronous=" + synchronousCall);

            MemoryShortageException resultException;
            if (!synchronousCall) {
                synchronized (_lock) {
                    if (aWriteTypeOperation)
                        _writeTypeEvict = true;
                    else
                        _readTypeEvict = true;

                    _lock.notify();
                }
            } else
                _runner.doEvict();

            resultException = aWriteTypeOperation ? _writeTypeMemoryException : _readTypeMemoryException;

            // throws MemoryShortageException if last eviction failed
            if (resultException != null) {
                createDumpIfAppropriate();
                throw resultException;
            }

        }

        public void stop() {
            synchronized (_lock) {
                if (_runner != null) {
                    _writeTypeMemoryException = null;
                    _readTypeMemoryException = null;
                    _readTypeEvict = false;
                    _writeTypeEvict = false;
                    _runner.shutdown();
                    _runner = null;
                }
            }
        }

        public void start() {
            synchronized (_lock) {
                if (_runner != null)
                    return;

                _runner = new EvictorRunner();
                _runner.start();
            }
        }

        /**
         * Async thread that tries to evict entries when called.
         *
         * @author guy
         * @since 7.0
         */
        private class EvictorRunner extends GSThread {

            private boolean _isShutdown = false;

            EvictorRunner() {
                super(_containerName + ":" + _spaceName + "-Evictor");
                setDaemon(true);
            }

            public void shutdown() {
                _isShutdown = true;
                this.interrupt();
            }

            @Override
            public void run() {

                while (!isInterrupted()) {
                    synchronized (_lock) {
                        try {
                            while (!_readTypeEvict && !_writeTypeEvict && !_isShutdown) {
                                if (_logger.isTraceEnabled())
                                    _logger.trace("Evictor waits for - call evict");

                                _lock.wait();
                            }
                            _readTypeEvict = false;
                            _writeTypeEvict = false;
                        } catch (InterruptedException ie) {

                            if (_logger.isTraceEnabled()) {
                                _logger.trace(this.getName() + " interrupted.", ie);
                            }

                            //Restore the interrupted status
                            interrupt();

                            //fall through
                            break;
                        } catch (Exception e) {
                            if (_logger.isWarnEnabled()) {
                                _logger.warn(e.toString(), e);
                            }
                        }
                        if (_isShutdown) // shutdown thread
                        {
                            return;
                        }
                    }
                    doEvict();
                }
            }

            //NOTE- synchronized method, no use in M.T. trying to fight over eviction
            private synchronized void doEvict() throws MemoryShortageException {
                double rate = getMemoryUsageRate(_callGC, false);

                if (_logger.isDebugEnabled())
                    _logger.debug("Evictor started to evict, rate=" + rate + " free-memory=" + _processMemoryManager.getFreeMemory() + " max-memory=" + _processMemoryManager.getMaximumMemory());

                try {
                    if (_cacheManager.isEvictableCachePolicy() && _evictionQuota > 0) {
                        // try to lower the memory level
                        for (int retryCount = 0; retryCount < _memoryRetryCount; retryCount++) {
                            if (rate <= _memoryUsageLowLevel) // no need to continue nor to check for shortage
                                return;

                            if (_logger.isDebugEnabled())
                                _logger.debug("SpaceName: " + _spaceName + " Cache eviction started: Available memory[%]" + rate);

                            //try to evict from cache, until situation mended
                            final int evicted = _cacheManager.evictBatch(getEvictionBatchSize());

                            if (_logger.isDebugEnabled())
                                _logger.debug("Batch evicted size=" + evicted);

                            if (evicted == 0) //nothing to evict
                                break;

                            rate = getMemoryUsageRate(_callGC, false);

                            if (_logger.isDebugEnabled())
                                _logger.debug("rate=" + rate + " free-memory=" + _processMemoryManager.getFreeMemory() + " max-memory=" + _processMemoryManager.getMaximumMemory());

                            if (rate <= _memoryUsageLowLevel) //reached lower level
                                return;

                            if (_logger.isDebugEnabled()) {
                                _logger.debug("SpaceName: " + _spaceName + " Cache eviction finished: Available memory[%]" + rate +
                                        " evicted all entries.");
                            }

                            try // goto sleep before next retry
                            {
                                Thread.sleep(_layoffTimeout);
                            } catch (InterruptedException ie) {
                                if (_logger.isTraceEnabled())
                                    _logger.trace(Thread.currentThread().getName() + " interrupted.", ie);

                                //Restore the interrupted status
                                Thread.currentThread().interrupt();

                                //fall through
                                break;
                            }
                            // if this is the last try call GC anyway
                            rate = getMemoryUsageRate(_callGC, false);
                        }// end for loop of retries

                        if (_logger.isDebugEnabled())
                            _logger.debug("Evictor finished to evict, rate=" + rate);

                    }// if m_CacheManager.m_CachePolicy == CacheManager.CACHE_POLICY_LRU
                } finally {
                    rate = getMemoryUsageRate(false, false);
                    MemoryShortageException[] res = shortageChecks(rate);
                    if (res == null) {
                        _readTypeMemoryException = null;
                        _writeTypeMemoryException = null;
                    } else {
                        _readTypeMemoryException = res[1];
                        if (res[1] != null)
                            setMonitorOnlyWriteOps(false);
                        _writeTypeMemoryException = res[0];
                    }
                }
            }
        }
    }
}
