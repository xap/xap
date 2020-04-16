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

package com.j_spaces.core.cache;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.j_spaces.core.Constants;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.IStoredList;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PersistentGC thread.
 *
 * Responsible for cleaning cache indices.
 */
@com.gigaspaces.api.InternalApi
public class PersistentGC extends GSThread implements Constants.CacheManager {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);
    private static final String OBJECT_CLASS = Object.class.getName();

    private final CacheManager _cacheManager;
    private boolean _shouldDie;
    private long _persistentGCInterval;
    private final Object _shutdownMonitor = new Object();

    PersistentGC(CacheManager cacheManager, SpaceConfigReader configReader) {
        super("Cache-PersistentGC");
        this.setDaemon(true);

        _cacheManager = cacheManager;

        try {
            _persistentGCInterval = configReader.getLongSpaceProperty(PERSISTENT_GC_INTERVAL_PROP, String.valueOf(PERSISTENT_GC_INTERVAL_DEFAULT));
        } catch (Exception ex) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to parse persistent GC interval (using default).", ex);
            _persistentGCInterval = PERSISTENT_GC_INTERVAL_DEFAULT;
        }
    }

    public void run() {
        while (!isInterrupted()) {
            try {
                synchronized (_shutdownMonitor) {
                    if (_shouldDie)
                        return;

                    if (_persistentGCInterval > 0)
                        _shutdownMonitor.wait(_persistentGCInterval);
                    else
                        _shutdownMonitor.wait();

                    if (_shouldDie)
                        return;
                }

                if (_persistentGCInterval <= 0)
                    continue;

                // clean index tables
                cleanIndexTables();
            } catch (InterruptedException ie) {
                if (_logger.isTraceEnabled()) {
                    _logger.trace(this.getName() + " interrupted.", ie);
                }

                //Restore the interrupted status
                interrupt();

                //fall through
                break;
            } catch (Exception ex) {
                if (_logger.isErrorEnabled()) {
                    _logger.error("Persistent gc caught error while cleaning up.", ex);
                }
            }
        } /* while (true) */
    }

    /**
     * Cleans indexes with no values in index tables.
     */
    private void cleanIndexTables() throws SAException {
        // get safe reference to type table
        IServerTypeDesc templateType = _cacheManager.getTypeManager().getServerTypeDesc(OBJECT_CLASS);
        IServerTypeDesc[] subTypeDescs = templateType.getAssignableTypes();

        // iterate over classes
        for (IServerTypeDesc subTypeDesc : subTypeDescs) {
            TypeData typeData = _cacheManager.getTypeData(subTypeDesc);
            if (typeData == null)
                continue;

            // iterate over indexed fields, this is constant hash
            final TypeDataIndex[] indexes = typeData.getIndexes();
            for (TypeDataIndex index : indexes) {
                // GS-7384, PersistentGC is no longer in charge for deleting entries indexes
                // iterate over entries indexes
                // checkEntries( fme.getNonUniqueEntriesStore());

                // iterate over RT templates indexes
                checkTemplates(index._RTTemplates);

                // iterate over Notify templates indexes
                checkTemplates(index._NTemplates);
            }

            //cater of uids index for notify templates
            checkEntries(typeData.getNotifyUidTemplates());

            //cater of uids index for RT templates
            checkEntries(typeData.getReadTakeUidTemplates());

        }//for( int numeration : bfsClassNumeration)
    }

    private void checkTemplates(ConcurrentHashMap<Object, IStoredList<TemplateCacheInfo>[]> map) throws SAException {
        for (Map.Entry<Object, IStoredList<TemplateCacheInfo>[]> entry : map.entrySet()) {
            IStoredList<TemplateCacheInfo>[] t_vec = entry.getValue();
            if (t_vec == null)
                continue;  //ignore- unclear null for concurrentHashMap
            IStoredList<TemplateCacheInfo> valuesSL = t_vec[0];
            if (valuesSL.invalidate())
                map.remove(entry.getKey(), t_vec);
        }
    }

    private <K, V> void checkEntries(ConcurrentHashMap<K, IStoredList<V>> map) {
        for (Map.Entry<K, IStoredList<V>> entry : map.entrySet()) {
            IStoredList<V> valuesSL = entry.getValue();
            if (valuesSL == null)
                continue;  //ignore
            if (valuesSL.invalidate())
                map.remove(entry.getKey(), valuesSL);
        }
    }

    /**
     * close persistentGC.
     */
    public void shutdown()
            throws SAException {
        synchronized (_shutdownMonitor) {
            _shouldDie = true;
            _shutdownMonitor.notifyAll();
        }

        try {
            // waits for this thread to shutdown.
            this.join();
        } catch (InterruptedException ex) {
            if (_logger.isDebugEnabled()) {
                _logger.debug(ex.toString(), ex);
            }

            throw new SAException("Failed shutting down " + getName(), ex);
        }
    }
}
