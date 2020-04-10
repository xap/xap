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

package com.gigaspaces.internal.cluster.node.impl.directPersistency.admin;

import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySyncHandler;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The synchronizing direct-persistency handler
 *
 * @author yechielf
 * @since 10.2
 */
/*
    syncList Admin handler
*/
@com.gigaspaces.api.InternalApi
public class DirectPersistencySyncListAdmin {

    private static final Object _lock = new Object();
    private static volatile Map<Long, PlatformLogicalVersion> _versions = new HashMap<Long, PlatformLogicalVersion>();

    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_DIRECT_PERSISTENCY);
    private final DirectPersistencySyncHandler _main;
    private DirectPersistencySyncAdminInfo _info;

    public DirectPersistencySyncListAdmin(DirectPersistencySyncHandler mainlist) {
        _main = mainlist;
    }

    public void initialize(long currentGeneration) {//set the platformversion according to generation
        PlatformLogicalVersion curversion = PlatformLogicalVersion.getLogicalVersion();
        //get the sync-admin record from ssd and add current version
        _info = _main.getListHandler().getIoHandler().getSyncAdminIfExists();
        if (_info != null) {
            integrate(_info, currentGeneration, curversion);
            _info = new DirectPersistencySyncAdminInfo(_versions);
            _main.getListHandler().getIoHandler().update(_info);
        } else {
            integrate(currentGeneration, curversion);
            _info = new DirectPersistencySyncAdminInfo(_versions);
            _main.getListHandler().getIoHandler().insert(_info);
        }
    }


    public static void integrate(DirectPersistencySyncAdminInfo info, long curGeneration, PlatformLogicalVersion curversion) {
        synchronized (_lock) {
            Map<Long, PlatformLogicalVersion> newmap = new HashMap<Long, PlatformLogicalVersion>(_versions);
            newmap.putAll(info.getVersions());
            newmap.put(curGeneration, curversion);
            _versions = newmap;
        }
    }

    public static void integrate(long curGeneration, PlatformLogicalVersion curversion) {
        synchronized (_lock) {
            Map<Long, PlatformLogicalVersion> newmap = new HashMap<Long, PlatformLogicalVersion>(_versions);
            newmap.put(curGeneration, curversion);
            _versions = newmap;
        }
    }


    public void cleanOlderGens(long curGeneration) {
        PlatformLogicalVersion curversion = PlatformLogicalVersion.getLogicalVersion();
        //get the sync-admin record from ssd and add current version
        Map<Long, PlatformLogicalVersion> newmap = new HashMap<Long, PlatformLogicalVersion>();
        newmap.put(curGeneration, curversion);
        _info = new DirectPersistencySyncAdminInfo(newmap);
        _main.getListHandler().getIoHandler().update(_info);
    }

    public static Map<Long, PlatformLogicalVersion> getVersions() {
        return _versions;
    }

}
