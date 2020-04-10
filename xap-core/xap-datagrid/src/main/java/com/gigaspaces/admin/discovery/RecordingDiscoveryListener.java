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

package com.gigaspaces.admin.discovery;

import com.gigaspaces.admin.ui.AdminUIThreadPool;

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DiscoveryListener that keeps a record of discovered and discarded ServiceRegistrar instances
 */
@com.gigaspaces.api.InternalApi
public class RecordingDiscoveryListener implements DiscoveryListener {

    private ArrayList discoveryTimes = new ArrayList();

    private static Logger LOGGER = LoggerFactory.getLogger(RecordingDiscoveryListener.class.getName());

    public RecordingDiscoveryListener() {

    }

    public void discovered(final DiscoveryEvent dEvent) {
        Runnable discoveryProcess = new Runnable() {
            public void run() {
                long t = System.currentTimeMillis();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(" --- BEGIN- discovered ");
                }

                Map groupsMap = dEvent.getGroups();
                Iterator<String[]> groupsIterator = groupsMap.values().iterator();
                for (int i = 0; i < dEvent.getRegistrars().length; i++) {
                    try {
                        String[] groups = groupsIterator.next();
                        ReggieStat rt = new ReggieStat(ReggieStat.DISCOVERED, t,
                                dEvent.getRegistrars()[i], groups);
                        ReggieStat existingStat = getReggieStat(rt);
                        if (existingStat != null &&
                                existingStat.type == ReggieStat.DISCARDED) {
                            rt.baseTime = existingStat.eventTime;
                        }
                        synchronized (discoveryTimes) {
                            discoveryTimes.add(rt);
                        }
                    } catch (RemoteException e) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info(
                                    "During recording discovery: " + e.toString(), e);
                        }
                    }
                }
                long endTime = System.currentTimeMillis();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(" --- Processing of discovering for event " + dEvent +
                            " took " + (endTime - t) + " msec.");
                }

            }
        };

        AdminUIThreadPool.getThreadPool().execute(discoveryProcess);
    }

    public void discarded(DiscoveryEvent dEvent) {
        long t = System.currentTimeMillis();
        ServiceRegistrar[] reggies = dEvent.getRegistrars();
        for (int i = 0; i < reggies.length; i++) {
            ReggieStat rStat = removeReggieStat(reggies[i].getServiceID());
            if (rStat != null) {
                rStat.eventTime = t;
                rStat.type = ReggieStat.DISCARDED;
                synchronized (discoveryTimes) {
                    discoveryTimes.add(rStat);
                }
            }
        }
    }

    /**
     * Get the collection of known discovered/discarded ServiceRegistrar discovery stats
     */
    public ReggieStat[] getReggieStats(int type) {
        if (type < ReggieStat.DISCOVERED || type > ReggieStat.DISCARDED)
            throw new IllegalArgumentException("bad type");
        ArrayList list = new ArrayList();
        synchronized (discoveryTimes) {
            for (Iterator it = discoveryTimes.iterator(); it.hasNext(); ) {
                ReggieStat rt = (ReggieStat) it.next();
                if (rt.type == type)
                    list.add(rt);
            }
        }
        return ((ReggieStat[]) list.toArray(new ReggieStat[list.size()]));
    }

    /**
     * Find and a ReggieStat based on the provided machine and port
     *
     * @param reggieStat A ReggieStat object, must not be null
     * @return A ReggieStat instance from the collection which matches the machine name and port the
     * provided ReggieStat has as properties
     */
    private ReggieStat getReggieStat(ReggieStat reggieStat) {
        if (reggieStat == null)
            throw new NullPointerException("reggieStat is null");
        ReggieStat rStat = null;
        synchronized (discoveryTimes) {
            for (Iterator it = discoveryTimes.iterator(); it.hasNext(); ) {
                ReggieStat rt = (ReggieStat) it.next();
                if (rt.machine.equals(reggieStat.machine) &&
                        rt.port == reggieStat.port &&
                        rt.groupsMatch(reggieStat)) {
                    rStat = rt;
                    break;
                }
            }
        }
        return (rStat);
    }

    /**
     * Find and remove a ReggieStat based on the provided ServiceID
     *
     * @param id The ServiceID for the ServiceRegistrar, must not be null
     * @return A ReggieStat instance that has been removed from the collection or null if not found
     */
    private ReggieStat removeReggieStat(ServiceID id) {
        if (id == null)
            throw new NullPointerException("id is null");
        ReggieStat rStat = null;
        synchronized (discoveryTimes) {
            for (Iterator it = discoveryTimes.iterator(); it.hasNext(); ) {
                ReggieStat rt = (ReggieStat) it.next();
                if (rt.serviceID.equals(id)) {
                    rStat = rt;
                    discoveryTimes.remove(rt);
                    break;
                }
            }
        }
        return (rStat);
    }
}
