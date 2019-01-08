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

package com.gigaspaces.lrmi.nio.watchdog;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.config.lrmi.nio.NIOConfiguration;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.LogLevel;
import com.gigaspaces.lrmi.ConnectionResource;
import com.gigaspaces.lrmi.nio.LrmiChannel;
import com.j_spaces.core.service.ServiceConfigLoader;
import com.j_spaces.kernel.SystemProperties;

import java.lang.ref.WeakReference;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Watchdog for network failure detection and handling idle threads.
 *
 * @author anna
 * @version 1.0
 * @since 5.0
 */
@com.gigaspaces.api.InternalApi
public class Watchdog extends GSThread {
    final static private Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_WATCHDOG);

    /** */
    private static final int MAX_RESOLUTION = 100;
    /** */
    private static final int MIN_RESOLUTION = 1;

    /**
     * Watch dog thread name
     */
    private static final String WATCHDOG = "Watchdog";

    /**
     * Flag to indicate that WatchedObject is not currently watched by the Watchdog
     */
    private static final int UNWATCHED = -1;

    private static ITransportConfig _config;

    /**
     * Groups indices
     */
    public static enum Group {
        REQUEST_GROUP, RESPONSE_GROUP, IDLE_GROUP
    }

    // Singleton watchdog
    private static Watchdog _watchdog;
    private static boolean _shutdown;

    final private WatchdogGroup[] _groups = new WatchdogGroup[Group.values().length];

    /**
     * Get WatchdogGroup and initialize singleton WatchDog daemon per JVM.
     */
    public static synchronized WatchdogGroup getGroup(Group group) {
        if (_watchdog == null)
            initWatchdog();

        return _watchdog._groups[group.ordinal()];
    }

    /*public static synchronized void setConfiguration(ITransportConfig config)
    {
		if(null == _config)
			_config = config;
	}*/

    /**
     * Initializes the Watchdog from System properties
     */
    private static void initWatchdog() {
        if (_shutdown)
            return;
        // Note: for simplification reasons, some of these properties have been hidden from the documentation.
        if (null == _config)
            _config = ServiceConfigLoader.getTransportConfiguration();

        //TODO do we need to expose this parameter as well?
        int timeoutResolution = Integer.parseInt(System.getProperty(SystemProperties.WATCHDOG_TIMEOUT_RESOLUTION,
                SystemProperties.WATCHDOG_TIMEOUT_RESOLUTION_DEFAULT));

        boolean protocolValidationEnabled = ((NIOConfiguration) _config).isProtocolValidationEnabled();

        _watchdog = new Watchdog(WATCHDOG,
                _config.getWatchdogRequestTimeout(),
                _config.getWatchdogListeningTimeout(),
                _config.getWatchdogIdleConnectionTimeout(),
                timeoutResolution,
                protocolValidationEnabled);
        _watchdog.start();

    }

    /**
     * Shuts down the watchdog if operation, this is irreversable process.
     */
    public static synchronized void shutdown() {
        _shutdown = true;
        Watchdog watchdog = _watchdog;
        if (watchdog != null)
            watchdog.interrupt();
    }

    /**
     * Constructor
     */
    public Watchdog(String name, long requestTimeout, long listeningTimeout,
                    long idleTimeout, int timeoutResolution, boolean protocolValidationEnabled) {
        super(name);

        // Idle timeout is 90% of the listening timeout
        int idleConnectionTimeout = Math.max((int) (listeningTimeout * 0.9), 1);

        int idleConnectionRetries = (int) (idleTimeout / idleConnectionTimeout - 1);

        timeoutResolution = inRange(timeoutResolution,
                MIN_RESOLUTION,
                MAX_RESOLUTION);

        if (_logger.isDebugEnabled()) {
            _logger.debug("Running watchdog with listening timeout="
                    + listeningTimeout + "sec,request timeout=" + requestTimeout
                    + " millis,idle connection timeout=" + idleConnectionTimeout
                    + " millis, retries=" + idleConnectionRetries
                    + ", timeout resolution=" + timeoutResolution + "%.");
        }

        _groups[Group.REQUEST_GROUP.ordinal()] = new WatchdogGroup(Group.REQUEST_GROUP.name(),
                requestTimeout,
                timeoutResolution,
                new RequestTimeoutObserver(requestTimeout));
        _groups[Group.RESPONSE_GROUP.ordinal()] = new WatchdogGroup(Group.RESPONSE_GROUP.name(),
                requestTimeout,
                timeoutResolution,
                new RequestResponseTimeoutObserver(requestTimeout, protocolValidationEnabled));
        _groups[Group.IDLE_GROUP.ordinal()] = new WatchdogGroup(Group.IDLE_GROUP.name(),
                idleConnectionTimeout,
                timeoutResolution,
                new IdleConnectionTimeoutObserver(idleConnectionRetries));

        setDaemon(true);
    }

    /**
     * Watchdog main method. Watchdog monitors several WatchdogGroups that have different timeout
     * settings. Therefore its sleeping timeout is calculated each time to be the next closest
     * running time of its groups. TODO verify that Watchdog processing time that is not taken into
     * consideration doesn't create a large timing error
     */
    @Override
    public void run() {
        long time = 0;

        while (!isInterrupted()) {
            try {

                long nextTime = calcNextTime();

                // Sleep for the defined period of time
                Thread.sleep(nextTime - time);

                // Update current time
                time = nextTime;

                // Timeout old connections
                for (int i = 0; i < _groups.length; i++) {
                    WatchdogGroup group = _groups[i];

                    if (group._doTimeout)
                        group.timeout();

                }

            } catch (InterruptedException ie) {

                if (_logger.isTraceEnabled()) {
                    _logger.trace(this.getName() + " interrupted.", ie);
                }

                //Restore the interrupted status
                interrupt();

                //fall through
                break;
            } catch (Throwable t) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(
                            "Unexpected exception in watchdog thread.",
                            t);
                }
            }
        }
    }

    /**
     * Calculate the next time Watchdog should run. All Watchdog groups are checked for the closest
     * time.
     */
    private long calcNextTime() {

        long nextTime = Long.MAX_VALUE;

        // Calculate next closest running time
        for (int i = 0; i < _groups.length; i++) {
            WatchdogGroup group = _groups[i];

            long gTime = group._time;
            long gWaitInterval = group._waitInterval;

            nextTime = Math.min(nextTime, gTime + gWaitInterval);
        }

        // Mark the groups that will be timeouted next clock tick
        for (int i = 0; i < _groups.length; i++) {
            WatchdogGroup group = _groups[i];

            group._doTimeout = (nextTime == group._time + group._waitInterval);

        }

        return nextTime;
    }

    /**
     * If given number is in given range - [min,max] - the number is returned unchanged, otherwise -
     * the closest number in range is returned : min or max
     */
    private int inRange(int number, int min, int max) {
        int result;

        result = Math.min(number, max);
        result = Math.max(result, min);

        return result;
    }

    /**
     * WatchdogGroup is a group of objects that are monitored by the Watchdog. Each WatchdogGroup
     * instance has its own configuration.
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    final public class WatchdogGroup {

        // Watchdog timeout in milliseconds
        final private long _timeout;

        // Last time this set was checked by the watchdog
        private long _time;

        // Watchdog timeout observer
        final private TimeoutObserver _observer;

        // Logical timer
        private int _timerLogical;

        // Watchdog timeout in logical clock ticks
        final private int _timeoutLogical;

        // Watchdog wait interval between checks
        final private long _waitInterval;

        // Objects monitored by the watchdog
        // ClientPeer is disconnected at finalize()
        // therefore WeakReference is used to avoid memory leaks and unclosed connections
        final private LinkedBlockingQueue<WeakReference<WatchedObject>> _watchedObjects =
                new LinkedBlockingQueue<WeakReference<WatchedObject>>();

        // Flag to indicate whether timeout should be executed,
        // next clock tick
        private boolean _doTimeout = false;

        // Group name
        final private String _name;

        /**
         * Constructor.
         *
         * @param timeout           watchdog group timeout in milliseconds
         * @param timeoutResolution in percents
         */
        public WatchdogGroup(String name, long timeout, int timeoutResolution,
                             TimeoutObserver observer) {
            super();

            _name = name;
            _observer = observer;
            _timeout = timeout;

            // Convert timeout resolution in percents to real time
            _waitInterval = (timeoutResolution * _timeout) / 100;

            // Calculate thread time to live for the logical timer
            _timeoutLogical = (int) (_timeout / _waitInterval);

        }

        /**
         * Timeout all thread that exceeded their time to live
         */
        private void timeout() throws Exception {
            // Update watchdog timer
            int t = _timerLogical++;

            // Update current time
            _time += _waitInterval;
            Map<WatchedObject, Collection<WatchedObject>> watchedObjects = new HashMap<WatchedObject, Collection<WatchedObject>>();
            for (Iterator<WeakReference<WatchedObject>> iter = _watchedObjects.iterator(); iter.hasNext(); ) {
                WeakReference<WatchedObject> w = iter.next();
                WatchedObject watched = w.get();

                if (watched == null || !watched.isInUse()) {
                    iter.remove();
                    continue;
                }
                int time = watched.getTime();

                // Check if the watched object is waiting or not
                // -1 means that the object is not currently under watch
                if (time == UNWATCHED)
                    continue;

                // Check if the watched object exceeded its waiting time
                if (t - time < _timeoutLogical)
                    continue;

                if (_logger.isDebugEnabled()) {
                    //log request timeouts as FINE, listening as FINER, idle as FINEST
                    LogLevel logLevel = LogLevel.TRACE;
                    if (Group.REQUEST_GROUP.name().equals(_name) ||
                            Group.RESPONSE_GROUP.name().equals(_name)) {
                        logLevel = LogLevel.DEBUG;
                    } else if (Group.IDLE_GROUP.name().equals(_name)) {
                        logLevel = LogLevel.TRACE;
                    }

                    logLevel.log(_logger, _name + " - " + (t - time) * _waitInterval
                            + " Timeout occurred, max allowed = " + _timeout);
                }

                add(watchedObjects, watched);

            }
            if (!watchedObjects.isEmpty())
                fireTimeoutOccured(watchedObjects);
        }

        private void add(Map<WatchedObject, Collection<WatchedObject>> watchedObjects, WatchedObject watched) {
            Collection<WatchedObject> bucket = watchedObjects.get(watched);
            if (null == bucket) {
                bucket = new LinkedList<WatchedObject>();
                watchedObjects.put(watched, bucket);
            }
            bucket.add(watched);
        }

        /**
         * Fire timeout event about the watched object to the <i>TimeoutObserver</i>
         *
         * @see TimeoutObserver
         * @see WatchedObject
         */
        protected void fireTimeoutOccured(Map<WatchedObject, Collection<WatchedObject>> watchedObjects) throws Exception {
            for (Collection<WatchedObject> bucket : watchedObjects.values()) {
                _observer.timeoutOccured(bucket);
            }
        }

        /**
         * Add given socket to the watched objects (request group)
         *
         * @param channel channel
         */
        public WatchedObject addRequestWatch(LrmiChannel channel, ConnectionResource client) {
            return addWatch(new WatchedObject(this, channel, client));

        }

        /**
         * Add given socket and cpeer to the watched objects (response group)
         *
         * @param channel channel
         */
        public WatchedObject addResponseWatch(LrmiChannel channel, ConnectionResource client) {
            return addWatch(new ResponseWatchedObject(this, channel, client));
        }

        /**
         * Add CPeer to the watched objects (idle group)
         */
        public WatchedObject addIdleWatch(ConnectionResource client) {
            return addWatch(new ClientWatchedObject(this, client));
        }

        /**
         * Adds given WatchedObject to the watched objects
         */
        private WatchedObject addWatch(WatchedObject watched) {
            try {
                _watchedObjects.put(new WeakReference<WatchedObject>(watched));
            } catch (InterruptedException e) {
                // if calling thread was interrupted
                // restore the interrupted status and return
                interrupt();
            }
            return watched;
        }

        /**
         * Remove given watched object from watch . WatchedObject is set as not in use and later
         * removed by the Watchdog.
         */
        public void removeWatch(WatchedObject watched) {
            watched.setInUse(false);

        }

        public long getTimeout() {
            return _timeout;
        }

    }

    /**
     * Object watched by the Watchdog - WatchedObject is used to watch NIO connections
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    static public class WatchedObject {

        // Last time this object was added to watch
        private int _time = UNWATCHED;
        // The socket to watch
        private final LrmiChannel _channel;

        // The client using the watched socket
        protected final ConnectionResource _client;

        // The group this object belongs to
        private final WatchdogGroup _watchdogGroup;

        // Used to deliver watchdog exceptions to the calling thread
        private Exception _exception;

        private volatile boolean _inUse = true;

        private String monitoringId;

        private long version = -1;

        /**
         * @param group
         * @param channel
         */
        public WatchedObject(WatchdogGroup group, LrmiChannel channel, ConnectionResource client) {
            _watchdogGroup = group;
            _channel = channel;
            _client = client;
        }

        public int getTime() {
            return _time;
        }

        /**
         * @return
         */
        public LrmiChannel getChannel() {
            return _channel;
        }

        public ConnectionResource getClient() {
            return _client;
        }

        /**
         * Start watching
         */
        public void startWatch() {
            _time = _watchdogGroup._timerLogical;
        }

        /**
         * Stop watching
         */
        public void stopWatch() {

            _time = UNWATCHED;
        }

        public Exception getException() {
            return _exception;
        }

        public void setException(Exception exception) {
            _exception = exception;
        }

        /**
         *
         * @return
         */
        public boolean isInUse() {
            return _inUse;
        }

        /**
         *
         * @param inUse
         */
        public void setInUse(boolean inUse) {
            _inUse = inUse;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof WatchedObject))
                return false;
            SocketAddress myAddress = _channel.getRemoteSocketAddress();
            SocketAddress hisAddress = ((WatchedObject) obj)._channel.getRemoteSocketAddress();
            if (myAddress != null)
                return myAddress.equals(hisAddress);
            return false;
        }

        @Override
        public int hashCode() {
            SocketAddress address = _channel.getRemoteSocketAddress();
            if (null == address)
                return 0;
            return address.hashCode();
        }

        // used to map watchdog logs to a specific method invocation
        public String getMonitoringId() {
            return monitoringId;
        }

        public void setMonitoringId(String monitoringId) {
            this.monitoringId = monitoringId;
        }

        public long getVersion() {
            return version;
        }

        public void incrementVersion() {
            version++;
        }
    }

    /**
     * Watched object for ResponseGroup, as response watch objects buckets always have a size of 1.
     * I.e, timeouts event relate to a specific invocation context
     *
     * @author Dan Kilman
     */
    final static class ResponseWatchedObject
            extends WatchedObject {
        public ResponseWatchedObject(WatchdogGroup group, LrmiChannel channel,
                                     ConnectionResource client) {
            super(group, channel, client);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
    }

    /**
     * A WatchedObject implementation for watching ClientPeer objects
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    final static class ClientWatchedObject
            extends WatchedObject {
        int _retries;

        public ClientWatchedObject(WatchdogGroup group, ConnectionResource client) {
            super(group, null, client);
        }

        /**
         * Stop watch and reset retries count
         */
        @Override
        public void stopWatch() {
            super.stopWatch();
            _retries = 0;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ClientWatchedObject))
                return false;
            return _client.equals(((ClientWatchedObject) obj)._client);
        }

        @Override
        public int hashCode() {
            return _client.hashCode();
        }

    }
}
