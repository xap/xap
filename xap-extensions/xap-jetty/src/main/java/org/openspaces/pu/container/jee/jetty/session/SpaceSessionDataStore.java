/*
 * Copyright (c) 2008-2019, GigaSpaces Technologies, Inc. All Rights Reserved.
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
package org.openspaces.pu.container.jee.jetty.session;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.query.IdQuery;
import com.j_spaces.core.client.SQLQuery;
import net.jini.core.lease.Lease;
import org.eclipse.jetty.server.session.AbstractSessionDataStore;
import org.eclipse.jetty.server.session.SessionContext;
import org.eclipse.jetty.server.session.SessionData;
import org.openspaces.core.GigaSpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @since 15.0.0
 * @author Niv Ingberg
 */
public class SpaceSessionDataStore extends AbstractSessionDataStore {
    private final GigaSpace gigaSpace;
    private final Logger logger = LoggerFactory.getLogger(SpaceSessionDataStore.class);

    private long lease = Lease.FOREVER;
    private String cacheKeyPrefix;

    public SpaceSessionDataStore(GigaSpace gigaSpace) {
        logger.debug("Created - gigaSpace={}", gigaSpace.getSpaceName());
        this.gigaSpace = gigaSpace;
    }

    @Override
    public void initialize(SessionContext context) throws Exception {
        logger.debug("initialize({})", context);
        super.initialize(context);
        this.cacheKeyPrefix = context.getCanonicalContextPath() + "_" + context.getVhost() + "_";
    }

    @Override
    public boolean isPassivating() {
        return true;
    }

    @Override
    public void doStore(String id, SessionData data, long lastSaveTime) throws Exception {
        logger.debug("doStore(id={})", id);
        gigaSpace.write(new SessionDataWrapper(toCacheKey(id), data), lease);
    }

    @Override
    public SessionData doLoad(String id) throws Exception {
        logger.debug("doLoad(id={})", id);
        SessionDataWrapper spaceSessionData = gigaSpace.readById(query(id));
        return spaceSessionData == null ? null : spaceSessionData.getSessionData();
    }

    @Override
    public Set<String> doGetExpired(Set<String> candidates) {
        logger.debug("doGetExpired()");
        if (candidates == null || candidates.isEmpty())
            return Collections.emptySet();

        /*
         *  Based on JDBCSessionDataStore.doGetExpired() algorithm:
         *  1. Select sessions managed by this node for our context that have expired
         *  2. Select sessions for any node or context that have expired
         *  at least 1 graceperiod since the last expiry check. If we haven't done previous expiry checks, then check
         *  those that have expired at least 3 graceperiod ago.
         */
        long managedExpiry = System.currentTimeMillis();
        long unmanagedExpiry = _lastExpiryCheckTime <= 0
                ? managedExpiry - (3 * (1000L * _gracePeriodSec))
                : _lastExpiryCheckTime - (1000L * _gracePeriodSec);

        AsyncFuture<HashSet<String>> future = gigaSpace.execute(new FindNonExpiredSessionsTask(candidates,
                _context.getCanonicalContextPath(), _context.getVhost(), managedExpiry, unmanagedExpiry));

        HashSet<String> result = new HashSet<>(candidates);
        try {
            HashSet<String> nonExpired = future.get();
            result.removeAll(nonExpired);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while doGetExpired (candidates={})", candidates);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.warn("Failed while doGetExpired: {} (candidates={})", e, candidates);
        }
        return result;
    }

    @Override
    public boolean exists(String id) throws Exception {
        logger.debug("exists(id={})", id);
        int result = gigaSpace.count(new SQLQuery<>(SessionDataWrapper.class,
                "id = ? AND (sessionData.expiry <= 0 OR sessionData.expiry > ?)",
                toCacheKey(id),
                System.currentTimeMillis()));
        return result != 0;
    }

    @Override
    public boolean delete(String id) throws Exception {
        logger.debug("delete(id={})", id);
        int result = gigaSpace.clear(query(id), gigaSpace.getDefaultClearModifiers());
        return result != 0;
    }

    @Override
    public SessionData newSessionData(String id, long created, long accessed, long lastAccessed, long maxInactiveMs) {
        logger.debug("newSessionData(id={})", id);
        return new SpaceSessionData(id, _context.getCanonicalContextPath(), _context.getVhost(), created, accessed, lastAccessed, maxInactiveMs);
    }

    public void setLease(long lease) {
        this.lease = lease;
    }

    protected IdQuery<SessionDataWrapper> query(String id) {
        return new IdQuery<>(SessionDataWrapper.class, toCacheKey(id));
    }

    private String toCacheKey(String id) {
        return cacheKeyPrefix + id;
    }
}
