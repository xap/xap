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


package org.openspaces.jee.sessions.jetty;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import com.gigaspaces.client.ClearModifiers;
import com.j_spaces.core.IJSpace;

import net.jini.core.lease.Lease;

import org.eclipse.jetty.server.session.AbstractSession;
import org.eclipse.jetty.server.session.AbstractSessionManager;
import org.eclipse.jetty.util.LazyList;
import org.eclipse.jetty.util.log.Log;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import com.j_spaces.core.client.SQLQuery;

/**
 * GigaspacesSessionManager
 *
 * A Jetty SessionManager where the session data is stored in a data grid "cloud".
 *
 * On each request, the session data is looked up in the "cloud" and brought into the local space
 * cache if doesn't already exist, and an entry put into the managers map of sessions. When the
 * request exists, any changes, including changes to the access time of the session are written back
 * out to the grid.
 */
public class GigaSessionManager extends AbstractSessionManager {

    private GigaSpace gigaSpace;

    private UrlSpaceConfigurer urlSpaceConfigurer;

    private long lease = Lease.FOREVER;

    protected long _scavengePeriodMs = TimeUnit.MINUTES.toMillis(5); //5mins

    protected int _scavengeCount = 0;

    protected long _savePeriodMs = TimeUnit.MINUTES.toMillis(1); //1mins

    private volatile static ScheduledExecutorService executorService;

    private volatile static int totalNumberOfScavangers = 0;

    // not static, we want to schedule one for each instance of the session manager
    private volatile ScheduledFuture scavengerFuture;

    private static final Object executorMonitor = new Object();

    private volatile int lastSessionCount = -1;

    private volatile long lastCountSessionsTime = System.currentTimeMillis();

    private long countSessionPeriod = TimeUnit.MINUTES.toMillis(5); // every 5 minutes do the sessions count

    /**
     * Start the session manager.
     */
    @Override
    public void doStart() throws Exception {
        if (gigaSpace == null) {
            gigaSpace = new GigaSpaceConfigurer(urlSpaceConfigurer).create();
        }

        if (_sessionIdManager == null) {
            _sessionIdManager = new GigaSessionIdManager(getSessionHandler().getServer());
            ((GigaSessionIdManager) _sessionIdManager).setSpace(gigaSpace);
        }
        if (_sessionIdManager instanceof GigaSessionIdManager) {
            if (((GigaSessionIdManager) _sessionIdManager).getSpace() == null) {
                ((GigaSessionIdManager) _sessionIdManager).setSpace(gigaSpace);
            }
        }

        synchronized (executorMonitor) {
            if (totalNumberOfScavangers == 0) {
                if (Log.getLog().isDebugEnabled())
                    Log.getLog().debug("Starting scavenger with period [" + _scavengePeriodMs + "ms]");
                executorService = Executors.newScheduledThreadPool(1);
            }
            totalNumberOfScavangers++;
            scavengerFuture = executorService.scheduleWithFixedDelay(new Runnable() {
                public void run() {
                    scavenge();
                }
            }, _scavengePeriodMs, _scavengePeriodMs, TimeUnit.MILLISECONDS);
        }

        super.doStart();
    }

    /**
     * Stop the session manager.
     */
    @Override
    public void doStop() throws Exception {
        synchronized (executorMonitor) {
            if (scavengerFuture != null) {
                scavengerFuture.cancel(true);
                if (--totalNumberOfScavangers == 0) {
                    if (Log.getLog().isDebugEnabled()) Log.getLog().debug("Stopping scavenger");
                    executorService.shutdown();
                }
            }
        }
        gigaSpace = null;
        if (urlSpaceConfigurer != null) {
            urlSpaceConfigurer.close();
        }
        super.doStop();
    }

    /**
     * How often an actual update of the session will be performed to the Space. Set in
     * <b>seconds</b> and defaults to <code>60</code> seconds.
     */
    public int getSavePeriod() {
        return (int) TimeUnit.MILLISECONDS.toSeconds(_savePeriodMs);
    }

    /**
     * How often an actual update of the session will be performed to the Space. Set in
     * <b>seconds</b> and defaults to <code>60</code> seconds.
     */
    public void setSavePeriod(int seconds) {
        if (seconds <= 0)
            seconds = 60;

        _savePeriodMs = TimeUnit.SECONDS.toMillis(seconds);
    }


    /**
     * How often the scavenger thread will run in order to check for expired sessions. Set in
     * <b>seconds</b> and defaults to <code>60 * 5</code> seconds (5 minutes).
     */
    public int getScavengePeriod() {
        return (int) TimeUnit.MILLISECONDS.toSeconds(_scavengePeriodMs);
    }

    /**
     * How often the scavenger thread will run in order to check for expired sessions. Set in
     * <b>seconds</b> and defaults to <code>60 * 5</code> seconds (5 minutes).
     */
    public void setScavengePeriod(int seconds) {
        if (seconds <= 0) {
            seconds = 60;
        }

        _scavengePeriodMs = TimeUnit.SECONDS.toMillis(seconds);
    }

    public void setCountSessionPeriod(int seconds) {
        this.countSessionPeriod = TimeUnit.SECONDS.toMillis(seconds);
    }

    public void setUrlSpaceConfigurer(UrlSpaceConfigurer urlSpaceConfigurer) {
        this.urlSpaceConfigurer = urlSpaceConfigurer;
    }

    public void setSpace(GigaSpace gigaSpace) {
        this.gigaSpace = gigaSpace;
    }

    public void setSpace(IJSpace space) {
        this.gigaSpace = new GigaSpaceConfigurer(space).create();
    }

    public GigaSpace getSpace() {
        return gigaSpace;
    }

    /**
     * The lease of the {@link org.openspaces.jee.sessions.jetty.SessionData} that is written to the
     * Space. Set in <b>seconds</b> and defaults to FOREVER.
     */
    public void setLease(long lease) {
        this.lease = TimeUnit.SECONDS.toMillis(lease);
    }

    /**
     * Get a session matching the id.
     *
     * Look in the grid to see if such a session exists, as it may have moved from another node.
     */
    @Override
    public AbstractSession getSession(String idInCluster) {

        // TODO do we really need to synchronize on (this) here? It used to be like that
        try {
            SessionData data = gigaSpace.readById(SessionData.newIdQuery(idInCluster));

            if (data == null) {
                if (Log.getLog().isDebugEnabled())
                    Log.getLog().debug("No session matching id [" + idInCluster + "]");
                return null;
            }
            if (Log.getLog().isDebugEnabled())
                Log.getLog().debug("Found matching session [" + idInCluster + "]");
            return new Session(data);
        } catch (Exception e) {
            Log.getLog().warn("Unable to load session", e);
            return null;
        }
    }


    @Override
    public Map getSessionMap() {
        // TODO we might want to read some sessions and give it back...
        return new HashMap();
    }

    @Override
    public int getSessions() {
        long now = System.currentTimeMillis();
        if (lastSessionCount == -1 || (now - lastCountSessionsTime) > countSessionPeriod) {
            try {
                lastSessionCount = gigaSpace.count(new SessionData());
            } catch (Exception e) {
                Log.getLog().warn("Failed to execute count of sessions", e);
            }
            lastCountSessionsTime = now;
        }
        return lastSessionCount;
    }

    @Override
    public void resetStats() {
        lastSessionCount = -1;
        super.resetStats();
    }

    @Override
    protected void shutdownSessions() throws Exception {
        //Do nothing - we don't want to remove and
        //invalidate all the sessions because this
        //method is called from doStop(), and just
        //because this context is stopping does not
        //mean that we should remove the session from
        //any other nodes
    }

    @Override
    protected AbstractSession newSession(HttpServletRequest request) {
        return new Session(request);
    }

    @Override
    protected boolean removeSession(String idInCluster) {
        try {
            return gigaSpace.clear(SessionData.newIdQuery(idInCluster), ClearModifiers.NONE) != 0;
        } catch (Exception e) {
            Log.getLog().warn("Failed to remove session with id [" + idInCluster + "]", e);
            return false;
        }
    }

    @Override
    public boolean removeSession(AbstractSession abstractSession, boolean invalidate) {
        if (!(abstractSession instanceof GigaSessionManager.Session))
            throw new IllegalStateException("Session is not a GigaspacesSessionManager.Session " + abstractSession);

        //TODO there was synchronize on both sessionIdManager and this here, do we really need it?

        String sessionId = getClusterId(abstractSession);
        boolean removed = removeSession(sessionId);
        if (removed) {
            _sessionIdManager.removeSession(abstractSession);
            if (invalidate)
                _sessionIdManager.invalidateAll(sessionId);
        }

        if (invalidate && _sessionListeners != null) {
            HttpSessionEvent event = new HttpSessionEvent(abstractSession);
            for (int i = LazyList.size(_sessionListeners); i-- > 0; )
                ((HttpSessionListener) LazyList.get(_sessionListeners, i)).sessionDestroyed(event);
        }
        if (!invalidate) {
            abstractSession.willPassivate();
        }
        return removed;
    }

    @Override
    protected void addSession(AbstractSession abstractSession) {
        if (abstractSession == null)
            return;

        if (!(abstractSession instanceof GigaSessionManager.Session))
            throw new IllegalStateException("Not a GigaspacesSessionManager.Session " + abstractSession);

        GigaSessionManager.Session session = (GigaSessionManager.Session) abstractSession;

        try {
            gigaSpace.write(session._data, lease);

        } catch (Exception e) {
            Log.getLog().warn("Problem writing new SessionData to space ", e);
        }
    }

    /**
     * Look for expired sessions that we know about in our session map, and double check with the
     * grid that it has really expired, or already been removed.
     */
    protected void scavenge() {
        //don't attempt to scavenge if we are shutting down
        if (isStopping() || isStopped())
            return;

        Thread thread = Thread.currentThread();
        ClassLoader origClassLoader = thread.getContextClassLoader();
        _scavengeCount++;

        try {
            if (_loader != null)
                thread.setContextClassLoader(_loader);
            long now = System.currentTimeMillis();
            if (Log.getLog().isDebugEnabled())
                Log.getLog().debug("Scavenging old sessions, expiring before: " + (now));
            Object[] expiredSessions;
            do {
                expiredSessions = gigaSpace.readMultiple(
                        new SQLQuery<SessionData>(SessionData.class, "expiryTime < ?", now), 100);
                for (int i = 0; i < expiredSessions.length; i++) {
                    if (Log.getLog().isDebugEnabled())
                        Log.getLog().debug("Timing out expired session " + expiredSessions[i]);
                    Session expiredSession = new Session((SessionData) expiredSessions[i]);
                    expiredSession.timeout();
                    if (Log.getLog().isDebugEnabled())
                        Log.getLog().debug("Expiring old session " + expiredSession._data);
                }
            } while (expiredSessions.length > 0);

            // force a count
            lastSessionCount = -1;
        } catch (Throwable t) {
            if (t instanceof ThreadDeath)
                throw ((ThreadDeath) t);

            Log.getLog().warn("Problem scavenging sessions", t);
        } finally {
            thread.setContextClassLoader(origClassLoader);
        }
    }

    /**
     * Session
     *
     * A session in memory of a Context. Adds behavior around SessionData.
     */
    public class Session extends AbstractSession {

        private final SessionData _data;

        private volatile boolean _dirty = false;

        /**
         * Session from a request.
         */
        protected Session(HttpServletRequest request) {
            super(GigaSessionManager.this, request);
            _data = new SessionData(getClusterId());
            _data.setMaxIdleMs(TimeUnit.SECONDS.toMillis(_dftMaxIdleSecs));
            _data.setExpiryTime(getMaxInactiveInterval() < 0 ? Long.MAX_VALUE : (System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(getMaxInactiveInterval())));
            _data.setCookieSet(0);

            Enumeration<String> attributeNames = getAttributeNames();
            HashMap<String, Object> attributes = new HashMap<String, Object>();
            while (attributeNames.hasMoreElements()) {
                String nextAttribute = attributeNames.nextElement();
                attributes.put(nextAttribute, request.getAttribute(nextAttribute));
            }
            _data.setAttributeMap(attributes);
            if (Log.getLog().isDebugEnabled())
                Log.getLog().debug("New Session from request, " + _data.toStringExtended());
        }

        protected Session(SessionData data) {
            super(GigaSessionManager.this, data.getCreated(), data.getAccessed(), data.getId());
            _data = data;
            for (Map.Entry<String, Object> attribute : data.getAttributeMap().entrySet()) {
                super.setAttribute(attribute.getKey(), attribute.getValue());
            }
            //Merges the two tables and make sure both SessionData and AbstractSessionManager.Session holds the same map
            Enumeration<String> attributeNames = getAttributeNames();
            HashMap<String, Object> attributes = new HashMap<String, Object>();
            while (attributeNames.hasMoreElements()) {
                String nextAttribute = attributeNames.nextElement();
                attributes.put(nextAttribute, super.getAttribute(nextAttribute));
            }
            _data.setAttributeMap(attributes);

            if (Log.getLog().isDebugEnabled())
                Log.getLog().debug("New Session from existing session data " + _data.toStringExtended());
        }

        @Override
        protected void cookieSet() {
            _data.setCookieSet(_data.getAccessed());
        }

        @Override
        public void setAttribute(String name, Object value) {
            super.setAttribute(name, value);
            if (value == null) {
                _data.getAttributeMap().remove(name);
            } else {
                _data.getAttributeMap().put(name, value);
            }
            _dirty = true;
        }

        @Override
        public void removeAttribute(String name) {
            super.removeAttribute(name);
            _data.getAttributeMap().remove(name);
            _dirty = true;
        }

        /**
         * Entry to session. Called by SessionHandler on inbound request and the session already
         * exists in this node's memory.
         */
        @Override
        protected boolean access(long time) {
            boolean access = super.access(time);
            _data.setLastAccessed(_data.getAccessed());
            _data.setAccessed(time);
            _data.setExpiryTime(getMaxInactiveInterval() < 0 ? Long.MAX_VALUE : (time + TimeUnit.SECONDS.toMillis(getMaxInactiveInterval())));
            return access;
        }

        /**
         * We override it here so we can reset the expiry time based on the
         */
        @Override
        public void setMaxInactiveInterval(int seconds) {
            super.setMaxInactiveInterval(seconds);
            _data.setExpiryTime(getMaxInactiveInterval() < 0 ? Long.MAX_VALUE : (System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(getMaxInactiveInterval())));
        }

        /**
         * Exit from session
         *
         * If the session attributes changed then always write the session to the cloud.
         *
         * If just the session access time changed, we don't always write out the session, because
         * the gigaspace will serialize the unchanged session attributes. To save on serialization
         * overheads, we only write out the session when only the access time has changed if the
         * time at which we last saved the session exceeds the chosen save interval.
         */
        @Override
        protected void complete() {
            super.complete();
            try {
                if (_dirty || (_data.getAccessed() - _data.getLastSaved()) >= (_savePeriodMs)) {
                    _data.setLastSaved(System.currentTimeMillis());
                    willPassivate();
                    gigaSpace.write(_data, lease);
                    if (Log.getLog().isDebugEnabled())
                        Log.getLog().debug("Wrote session " + _data.toStringExtended());

                    didActivate();
                    if (Log.getLog().isDebugEnabled())
                        Log.getLog().debug("Dirty=" + _dirty + ", accessed-saved=" + _data.getAccessed() + "-" + _data.getLastSaved() + ", savePeriodMs=" + _savePeriodMs);
                }
            } catch (Exception e) {
                Log.getLog().warn("Problem persisting changed session data id=" + getId(), e);
            } finally {
                _dirty = false;
            }
        }

        @Override
        protected void timeout() throws IllegalStateException {
            if (Log.getLog().isDebugEnabled())
                Log.getLog().debug("Timing out session id=" + getClusterId());
            super.timeout();
        }
    }
}
