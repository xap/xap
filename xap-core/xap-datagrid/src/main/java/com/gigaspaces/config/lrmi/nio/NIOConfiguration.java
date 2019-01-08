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


package com.gigaspaces.config.lrmi.nio;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.lrmi.nio.PAdapter;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.TimeUnitProperty;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides configuration object of NIO communication transport protocol.
 *
 * @author Igor Goldenberg
 * @see com.gigaspaces.lrmi.GenericExporter
 * @since 5.2
 **/

public class NIOConfiguration implements ITransportConfig, Cloneable, Externalizable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(NIOConfiguration.class.getName());

    private static final int DEFAULT_MIN_THREADS = 1;
    private static final int DEFAULT_MAX_THREADS = 128;
    private static final int DEFAULT_THREAD_QUEUE_SIZE = Integer.MAX_VALUE;
    private static final int DEFAULT_MAX_CONN_POOL = 1024;
    private static final String DEFAULT_BIND_PORT = "0";
    private static final int DEFAULT_SLOW_CONSUMER_LATENCY = 500;
    private static final int DEFAULT_SLOW_CONSUMER_TP = 5000;
    private static final int DEFAULT_SLOW_CONSUMER_RETRIES = 3;
    private static final int DEFAULT_SLOW_CONSUMER_READ_TIMEOUT = 2000;
    private static final boolean DEFAULT_SLOW_CONSUMER_ENABLED = false;

    private int _minThreads = DEFAULT_MIN_THREADS;
    private int _maxThreads = DEFAULT_MAX_THREADS;
    private int _threadsQueueSize = DEFAULT_THREAD_QUEUE_SIZE;
    private int _maxConnPool = DEFAULT_MAX_CONN_POOL;
    private String _bindPort = DEFAULT_BIND_PORT; // any anonymous port
    private String _bindHostName;
    private boolean _blockingConnection = true; // set this client connection as blocking
    private int _slowConsumerLatency = DEFAULT_SLOW_CONSUMER_LATENCY;
    private int _slowConsumerThroughput = DEFAULT_SLOW_CONSUMER_TP;
    private int _slowConsumerRetries = DEFAULT_SLOW_CONSUMER_RETRIES;
    private int _slowConsumerReadTimeout = DEFAULT_SLOW_CONSUMER_READ_TIMEOUT;

    /**
     * LRMI number of read selector threads default = 4.
     *
     * @see SystemProperties#LRMI_READ_SELECTOR_THREADS
     */
    private int _readSelectorThreads = SystemProperties.LRMI_READ_SELECTOR_THREADS_DEFAULT;

    // This parameters are initialized to -1 so the system properties won't be parsed during serialization.
    /**
     * LRMI Watchdog parameters.
     *
     * @see SystemProperties#WATCHDOG_REQUEST_TIMEOUT_DEFAULT
     */
    private long _watchdogRequestTimeout = -1;
    /**
     * @see SystemProperties#WATCHDOG_LISTENING_TIMEOUT_DEFAULT
     */
    private long _watchdogListeningTimeout = -1;
    /**
     * @see SystemProperties#WATCHDOG_IDLE_CONNECTION_TIMEOUT_DEFAULT
     */
    private long _watchdogIdleConnectionTimeout = -1;

    private static final long WATCHDOG_REQUEST_TIMEOUT_DEFAULT = TimeUnitProperty.getParsedValue(SystemProperties.WATCHDOG_REQUEST_TIMEOUT_DEFAULT);

    private static final long WATCHDOG_LISTENING_TIMEOUT_DEFAULT = TimeUnitProperty.getParsedValue(SystemProperties.WATCHDOG_LISTENING_TIMEOUT_DEFAULT);

    private static final long WATCHDOG_IDLE_CONNECTION_TIMEOUT_DEFAULT = TimeUnitProperty.getParsedValue(SystemProperties.WATCHDOG_IDLE_CONNECTION_TIMEOUT_DEFAULT);

    private static final long LRMI_CONNECT_TIMEOUT_DEFAULT = TimeUnitProperty.getParsedValue(SystemProperties.LRMI_CONNECT_TIMEOUT_DEFAULT);

    /**
     * LRMI ThreadPool idle timeout.
     *
     * @see SystemProperties#LRMI_THREADPOOL_IDLE_TIMEOUT_DEFAULT
     */
    private long _threadPoolIdleTimeout = SystemProperties.LRMI_THREADPOOL_IDLE_TIMEOUT_DEFAULT;
    /**
     * The timeout on lrmi socket connect.
     *
     * @see SystemProperties#LRMI_CONNECT_TIMEOUT_DEFAULT
     */
    private long _socketConnectTimeout = -1;

    //We dont need to serialize this stuff and it may cause backward issue
    private transient int _systemPriorityQueueCapacity = SystemProperties.LRMI_SYSTEM_PRIORITY_QUEUE_CAPACITY_DEFAULT;
    private transient long _systemPriorityThreadIdleTimeout = SystemProperties.LRMI_SYSTEM_PRIORITY_THREAD_IDLE_TIMEOUT;
    private transient int _systemPriorityMinThreads = SystemProperties.LRMI_SYSTEM_PRIORITY_MIN_THREADS_DEFAULT;
    private transient int _systemPriorityMaxThreads = SystemProperties.LRMI_SYSTEM_PRIORITY_MAX_THREADS_DEFAULT;

    private transient int _customQueueCapacity = SystemProperties.LRMI_CUSTOM_QUEUE_CAPACITY_DEFAULT;
    private transient long _customThreadIdleTimeout = SystemProperties.LRMI_CUSTOM_THREAD_IDLE_TIMEOUT;
    private transient int _customMinThreads = SystemProperties.LRMI_CUSTOM_MIN_THREADS_DEFAULT;
    private transient int _customMaxThreads = SystemProperties.LRMI_CUSTOM_MAX_THREADS_DEFAULT;

    private boolean _protocolValidationEnabled;

    private interface BitMap {
        int MIN_THREADS = 1 << 0;
        int MAX_THREADS = 1 << 1;
        int THREAD_QUEUE_SIZE = 1 << 2;
        int MAX_CONN_POOL = 1 << 3;
        int BIND_PORT = 1 << 4;
        int BIND_HOST_NAME = 1 << 5;
        int BLOCKING_CONNECTION = 1 << 6;
        int SLOW_CONSUMER_LATENCY = 1 << 7;
        int SLOW_CONSUMER_TP = 1 << 8;
        int SLOW_CONSUMER_RETRIES = 1 << 9;
        int READ_SELECTOT_THREADS = 1 << 10;
        int WATCHDOG_REQUEST_TIMEOUT = 1 << 11;
        int WATCHDOG_LISTENING_TIMEOUT = 1 << 12;
        int WATCHDOG_IDLE_CONNECTION_TIMEOUT = 1 << 13;
        int THREAD_POOL_IDLE_TIMEOUT = 1 << 14;
        int SOCKET_CONNECT_TIMEOUT = 1 << 15;
        int SLOW_CONSUMER_READ_TIMEOUT = 1 << 16;
        int PROTOCOL_VALIDATION_ENABLED = 1 << 17;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        int flags = buildFlags();

        out.writeInt(flags);
        if (_minThreads != DEFAULT_MIN_THREADS) {
            out.writeInt(_minThreads);
        }
        if (_maxThreads != DEFAULT_MAX_THREADS) {
            out.writeInt(_maxThreads);
        }
        if (_threadsQueueSize != DEFAULT_THREAD_QUEUE_SIZE) {
            out.writeInt(_threadsQueueSize);
        }
        if (_maxConnPool != DEFAULT_MAX_CONN_POOL) {
            out.writeInt(_maxConnPool);
        }
        if (!DEFAULT_BIND_PORT.equals(_bindPort)) {
            out.writeUTF(_bindPort);
        }
        if (_bindHostName != null) {
            out.writeUTF(_bindHostName);
        }
        if (_slowConsumerLatency != DEFAULT_SLOW_CONSUMER_LATENCY) {
            out.writeInt(_slowConsumerLatency);
        }
        if (_slowConsumerThroughput != DEFAULT_SLOW_CONSUMER_TP) {
            out.writeInt(_slowConsumerThroughput);
        }
        if (_slowConsumerRetries != DEFAULT_SLOW_CONSUMER_RETRIES) {
            out.writeInt(_slowConsumerRetries);
        }
        if (_readSelectorThreads != SystemProperties.LRMI_READ_SELECTOR_THREADS_DEFAULT) {
            out.writeInt(_readSelectorThreads);
        }
        if (_watchdogRequestTimeout != WATCHDOG_REQUEST_TIMEOUT_DEFAULT) {
            out.writeLong(_watchdogRequestTimeout);
        }
        if (_watchdogListeningTimeout != WATCHDOG_LISTENING_TIMEOUT_DEFAULT) {
            out.writeLong(_watchdogListeningTimeout);
        }
        if (_watchdogIdleConnectionTimeout != WATCHDOG_IDLE_CONNECTION_TIMEOUT_DEFAULT) {
            out.writeLong(_watchdogIdleConnectionTimeout);
        }
        if (_threadPoolIdleTimeout != SystemProperties.LRMI_THREADPOOL_IDLE_TIMEOUT_DEFAULT) {
            out.writeLong(_threadPoolIdleTimeout);
        }
        if (_socketConnectTimeout != LRMI_CONNECT_TIMEOUT_DEFAULT) {
            out.writeLong(_socketConnectTimeout);
        }
        if (_slowConsumerReadTimeout != DEFAULT_SLOW_CONSUMER_READ_TIMEOUT) {
            out.writeInt(_slowConsumerReadTimeout);
        }
    }

    private int buildFlags() {
        int flags = 0;
        if (_minThreads != DEFAULT_MIN_THREADS) {
            flags |= BitMap.MIN_THREADS;
        }
        if (_maxThreads != DEFAULT_MAX_THREADS) {
            flags |= BitMap.MAX_THREADS;
        }
        if (_threadsQueueSize != DEFAULT_THREAD_QUEUE_SIZE) {
            flags |= BitMap.THREAD_QUEUE_SIZE;
        }
        if (_maxConnPool != DEFAULT_MAX_CONN_POOL) {
            flags |= BitMap.MAX_CONN_POOL;
        }
        if (!DEFAULT_BIND_PORT.equals(_bindPort)) {
            flags |= BitMap.BIND_PORT;
        }
        if (_bindHostName != null) {
            flags |= BitMap.BIND_HOST_NAME;
        }
        if (_blockingConnection) {
            flags |= BitMap.BLOCKING_CONNECTION;
        }
        if (_slowConsumerLatency != DEFAULT_SLOW_CONSUMER_LATENCY) {
            flags |= BitMap.SLOW_CONSUMER_LATENCY;
        }
        if (_slowConsumerThroughput != DEFAULT_SLOW_CONSUMER_TP) {
            flags |= BitMap.SLOW_CONSUMER_TP;
        }
        if (_slowConsumerRetries != DEFAULT_SLOW_CONSUMER_RETRIES) {
            flags |= BitMap.SLOW_CONSUMER_RETRIES;
        }
        if (_readSelectorThreads != SystemProperties.LRMI_READ_SELECTOR_THREADS_DEFAULT) {
            flags |= BitMap.READ_SELECTOT_THREADS;
        }
        if (_watchdogRequestTimeout != WATCHDOG_REQUEST_TIMEOUT_DEFAULT) {
            flags |= BitMap.WATCHDOG_REQUEST_TIMEOUT;
        }
        if (_watchdogListeningTimeout != WATCHDOG_LISTENING_TIMEOUT_DEFAULT) {
            flags |= BitMap.WATCHDOG_LISTENING_TIMEOUT;
        }
        if (_watchdogIdleConnectionTimeout != WATCHDOG_IDLE_CONNECTION_TIMEOUT_DEFAULT) {
            flags |= BitMap.WATCHDOG_IDLE_CONNECTION_TIMEOUT;
        }
        if (_threadPoolIdleTimeout != SystemProperties.LRMI_THREADPOOL_IDLE_TIMEOUT_DEFAULT) {
            flags |= BitMap.THREAD_POOL_IDLE_TIMEOUT;
        }
        if (_socketConnectTimeout != LRMI_CONNECT_TIMEOUT_DEFAULT) {
            flags |= BitMap.SOCKET_CONNECT_TIMEOUT;
        }
        if (_slowConsumerReadTimeout != DEFAULT_SLOW_CONSUMER_READ_TIMEOUT) {
            flags |= BitMap.SLOW_CONSUMER_READ_TIMEOUT;
        }
        if (_protocolValidationEnabled) {
            flags |= BitMap.PROTOCOL_VALIDATION_ENABLED;
        }
        return flags;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int flags = in.readInt();
        if ((flags & BitMap.MIN_THREADS) != 0) {
            _minThreads = in.readInt();
        } else {
            _minThreads = DEFAULT_MIN_THREADS;
        }
        if ((flags & BitMap.MAX_THREADS) != 0) {
            _maxThreads = in.readInt();
        } else {
            _maxThreads = DEFAULT_MAX_THREADS;
        }
        if ((flags & BitMap.THREAD_QUEUE_SIZE) != 0) {
            _threadsQueueSize = in.readInt();
        } else {
            _threadsQueueSize = DEFAULT_THREAD_QUEUE_SIZE;
        }
        if ((flags & BitMap.MAX_CONN_POOL) != 0) {
            _maxConnPool = in.readInt();
        } else {
            _maxConnPool = DEFAULT_MAX_CONN_POOL;
        }
        if ((flags & BitMap.BIND_PORT) != 0) {
            _bindPort = in.readUTF();
        } else {
            _bindPort = DEFAULT_BIND_PORT;
        }
        if ((flags & BitMap.BIND_HOST_NAME) != 0) {
            _bindHostName = in.readUTF();
        }
        _blockingConnection = ((flags & BitMap.BLOCKING_CONNECTION) != 0);
        if ((flags & BitMap.SLOW_CONSUMER_LATENCY) != 0) {
            _slowConsumerLatency = in.readInt();
        } else {
            _slowConsumerLatency = DEFAULT_SLOW_CONSUMER_LATENCY;
        }
        if ((flags & BitMap.SLOW_CONSUMER_TP) != 0) {
            _slowConsumerThroughput = in.readInt();
        } else {
            _slowConsumerThroughput = DEFAULT_SLOW_CONSUMER_TP;
        }
        if ((flags & BitMap.SLOW_CONSUMER_RETRIES) != 0) {
            _slowConsumerRetries = in.readInt();
        } else {
            _slowConsumerRetries = DEFAULT_SLOW_CONSUMER_RETRIES;
        }
        if ((flags & BitMap.READ_SELECTOT_THREADS) != 0) {
            _readSelectorThreads = in.readInt();
        } else {
            _readSelectorThreads = SystemProperties.LRMI_READ_SELECTOR_THREADS_DEFAULT;
        }
        if ((flags & BitMap.WATCHDOG_REQUEST_TIMEOUT) != 0) {
            _watchdogRequestTimeout = in.readLong();
        } else {
            _watchdogRequestTimeout = WATCHDOG_REQUEST_TIMEOUT_DEFAULT;
        }
        if ((flags & BitMap.WATCHDOG_LISTENING_TIMEOUT) != 0) {
            _watchdogListeningTimeout = in.readLong();
        } else {
            _watchdogListeningTimeout = WATCHDOG_LISTENING_TIMEOUT_DEFAULT;
        }
        if ((flags & BitMap.WATCHDOG_IDLE_CONNECTION_TIMEOUT) != 0) {
            _watchdogIdleConnectionTimeout = in.readLong();
        } else {
            _watchdogIdleConnectionTimeout = WATCHDOG_IDLE_CONNECTION_TIMEOUT_DEFAULT;
        }
        if ((flags & BitMap.THREAD_POOL_IDLE_TIMEOUT) != 0) {
            _threadPoolIdleTimeout = in.readLong();
        } else {
            _threadPoolIdleTimeout = SystemProperties.LRMI_THREADPOOL_IDLE_TIMEOUT_DEFAULT;
        }
        if ((flags & BitMap.SOCKET_CONNECT_TIMEOUT) != 0) {
            _socketConnectTimeout = in.readLong();
        } else {
            _socketConnectTimeout = LRMI_CONNECT_TIMEOUT_DEFAULT;
        }
        if ((flags & BitMap.SLOW_CONSUMER_READ_TIMEOUT) != 0) {
            _slowConsumerReadTimeout = in.readInt();
        } else {
            _slowConsumerReadTimeout = DEFAULT_SLOW_CONSUMER_READ_TIMEOUT;
        }
        if ((flags & BitMap.PROTOCOL_VALIDATION_ENABLED) != 0) {
            _protocolValidationEnabled = true;
        }
    }

    /**
     * Creates a new instance of NIO configuration based on system properties.
     */
    public static NIOConfiguration create() {
        String bindHost = SystemInfo.singleton().network().getHostId();
        String bindPort = System.getProperty("com.gs.transport_protocol.lrmi.bind-port", "8888");

        int minThreads = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.min-threads", "1"));
        int maxThreads = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.max-threads", "128"));
        int maxConnPool = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.max-conn-pool", "1024"));
        int readSelectorThreads = Integer.parseInt(System.getProperty(SystemProperties.LRMI_READ_SELECTOR_THREADS, String.valueOf(SystemProperties.LRMI_READ_SELECTOR_THREADS_DEFAULT)));

        //LRMI Watchdog parameters
        String watchdogRequestTimeout = System.getProperty(SystemProperties.WATCHDOG_REQUEST_TIMEOUT, SystemProperties.WATCHDOG_REQUEST_TIMEOUT_DEFAULT);
        String watchdogListeningTimeout = System.getProperty(SystemProperties.WATCHDOG_LISTENING_TIMEOUT, SystemProperties.WATCHDOG_LISTENING_TIMEOUT_DEFAULT);
        String watchdogIdleConnectionTimeout = System.getProperty(SystemProperties.WATCHDOG_IDLE_CONNECTION_TIMEOUT, SystemProperties.WATCHDOG_IDLE_CONNECTION_TIMEOUT_DEFAULT);

        //LRMI ThreadPool idle timeout default is 5 min.
        long threadPoolIdleTimeout = Long.parseLong(System.getProperty("com.gs.transport_protocol.lrmi.threadpool.idle_timeout", "300000"));
        int threadPoolQueueSize = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.threadpool.queue-size", Integer.toString(Integer.MAX_VALUE)));

        //LRMI timeout on the Socket connect. Default connect timeout in seconds is 30.
        String socketConnectTimeout = System.getProperty(SystemProperties.LRMI_CONNECT_TIMEOUT, SystemProperties.LRMI_CONNECT_TIMEOUT_DEFAULT);

        //Slow consumer parameters
        String slowConsumerEnabledKey = "com.gs.transport_protocol.lrmi.slow-consumer.enabled";
        boolean slowConsumerEnabled = Boolean.parseBoolean(System.getProperty(slowConsumerEnabledKey, String.valueOf(DEFAULT_SLOW_CONSUMER_ENABLED)));
        //If slow consumer is not enabled, reset slow consumer throughput to 0 because this is how the mechanism recognizes it is not enabled
        String slowConsumerThroughputKey = "com.gs.transport_protocol.lrmi.slow-consumer.throughput";
        int slowConsumerThroughput = Integer.parseInt(System.getProperty(slowConsumerThroughputKey, slowConsumerEnabled ? String.valueOf(DEFAULT_SLOW_CONSUMER_TP) : "0"));
        if (slowConsumerEnabled && slowConsumerThroughput == 0)
            throw new IllegalArgumentException("Contradicting slow consumer configuration: slow consumer is enabled (" + slowConsumerEnabledKey + ") but slow consumer throughput (" + slowConsumerThroughputKey + ") is explicitly set to 0");
        if (!slowConsumerEnabled && slowConsumerThroughput > 0)
            throw new IllegalArgumentException("Contradicting slow consumer configuration: slow consumer is disabled (" + slowConsumerEnabledKey + ") but slow consumer throughput (" + slowConsumerThroughputKey + ") is explicitly set to " + slowConsumerThroughput + ", this property should not be set when slow consumer is disabled");
        int slowConsumerRetries = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.slow-consumer.retries", String.valueOf(DEFAULT_SLOW_CONSUMER_RETRIES)));
        int slowConsumerLatency = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.slow-consumer.latency", String.valueOf(DEFAULT_SLOW_CONSUMER_LATENCY)));
        int slowConsumerReadTimeout = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.slow-consumer.read-timeout", String.valueOf(DEFAULT_SLOW_CONSUMER_READ_TIMEOUT)));

        int systemPriorityQueueCapacity = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.system-priority.threadpool.queue-size", String.valueOf(SystemProperties.LRMI_SYSTEM_PRIORITY_QUEUE_CAPACITY_DEFAULT)));
        long systemPriorityThreadPoolIdleTimeout = Long.parseLong(System.getProperty("com.gs.transport_protocol.lrmi.system-priority.threadpool.idle_timeout", String.valueOf(SystemProperties.LRMI_SYSTEM_PRIORITY_THREAD_IDLE_TIMEOUT)));
        int systemPriorityMinThreads = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.system-priority.threadpool.min-threads", String.valueOf(SystemProperties.LRMI_SYSTEM_PRIORITY_MIN_THREADS_DEFAULT)));
        int systemPriorityMaxThreads = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.system-priority.threadpool.max-threads", String.valueOf(SystemProperties.LRMI_SYSTEM_PRIORITY_MAX_THREADS_DEFAULT)));
        ;

        int customQueueCapacity = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.custom.threadpool.queue-size", String.valueOf(SystemProperties.LRMI_CUSTOM_QUEUE_CAPACITY_DEFAULT)));
        long customThreadPoolIdleTimeout = Long.parseLong(System.getProperty("com.gs.transport_protocol.lrmi.custom.threadpool.idle_timeout", String.valueOf(SystemProperties.LRMI_CUSTOM_THREAD_IDLE_TIMEOUT)));
        int customMinThreads = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.custom.threadpool.min-threads", String.valueOf(SystemProperties.LRMI_CUSTOM_MIN_THREADS_DEFAULT)));
        int customMaxThreads = Integer.parseInt(System.getProperty("com.gs.transport_protocol.lrmi.custom.threadpool.max-threads", String.valueOf(SystemProperties.LRMI_CUSTOM_MAX_THREADS_DEFAULT)));
        boolean protocolValidationEnabled = !Boolean.getBoolean(SystemProperties.LRMI_PROTOCOL_VALIDATION_DISABLED);

        NIOConfiguration nioConfiguration = new NIOConfiguration(minThreads, /* min executors threads */
                maxThreads, /* max executors threads */
                maxConnPool, /* maxConnPool */
                bindHost, /*  if null resolves to the localhost IP address */
                bindPort, /* if 0 any next free port will be used for incoming client requests.*/
                threadPoolQueueSize,
                slowConsumerThroughput,
                slowConsumerLatency,
                slowConsumerRetries,
                slowConsumerReadTimeout,
                readSelectorThreads,
                watchdogRequestTimeout,
                watchdogListeningTimeout,
                watchdogIdleConnectionTimeout,
                threadPoolIdleTimeout,
                socketConnectTimeout,
                systemPriorityQueueCapacity,
                systemPriorityThreadPoolIdleTimeout,
                systemPriorityMinThreads,
                systemPriorityMaxThreads,
                customQueueCapacity,
                customThreadPoolIdleTimeout,
                customMinThreads,
                customMaxThreads,
                protocolValidationEnabled
        );

        if (logger.isDebugEnabled()) {
            logger.debug(String.valueOf(nioConfiguration));
        }
        return nioConfiguration;
    }

    /**
     * Constructor.
     *
     * @param minThreads                  Maintains thread pool in Client and Server that manage
     *                                    incoming remote requests. The thread pool size is
     *                                    increased each time with one additional thread and shrinks
     *                                    when existing threads are not used for 5 minutes. This
     *                                    parameter specifies the minimum size of this thread pool.
     * @param maxThreads                  This parameter specifies the maximum size of this thread
     *                                    pool. You should make sure the pool max size will
     *                                    accommodate the maximum number of concurrent requests to
     *                                    the serverEndPoint. When the pool is exhausted and all
     *                                    threads are consumed to process incoming requests,
     *                                    additional requests will be blocked until existing
     *                                    requested processing will be completed.
     * @param maxConnPool                 Client connection pool maximum size to server. Starts with
     *                                    1 connection.
     * @param bindHostName                The host address the server socket is bound to. Relevant
     *                                    for the multi-NIC environment. If <code>null</code>
     *                                    resolves to the localhost IP address.
     * @param bindPort                    the bind port used for incoming client requests. The
     *                                    server port is set by default to 0, which means next free
     *                                    port. This means that whenever the serverEndPoint is
     *                                    launched it allocates one of the available ports. Define a
     *                                    specific port value to enforce a specific port for the
     *                                    serverEndPoint.
     * @param threadsQueueSize            Maintains thread pool queue size in Client and Server that
     *                                    manage incoming remote requests. Once the queue sized is
     *                                    reached the incoming messages are blocked.
     * @param slowConsumerThroughput      a client that its network throughput doesn't reach this
     *                                    limit will be suspected as slow.
     * @param slowConsumerLatency         a client that is suspected as slow that will not be
     *                                    recover after this time will be disconnected.
     * @param slowConsumerRetries         the number of retries to recover a client that is
     *                                    suspected as slow.
     * @param slowConsumerReadtimeout     the allowed time to read a reply until it is considered
     *                                    slow consuming
     * @param readSelectorThreads         LRMI number of read selector threads default = 1.
     * @param watchdogRequestTimeout      LRMI Watchdog request timeout.
     * @param watchdogListeningTimeout    LRMI Watchdog Listening timeout.
     * @param threadPoolIdleTimeout       LRMI ThreadPool idle timeout
     * @param socketConnectTimeout        The timeout on lrmi socket connect.
     * @param systemPriorityQueueCapacity TODO
     * @param protocolValidationEnabled   TODO
     * @see SystemProperties#LRMI_READ_SELECTOR_THREADS
     * @see SystemProperties#WATCHDOG_REQUEST_TIMEOUT_DEFAULT
     * @see SystemProperties#WATCHDOG_LISTENING_TIMEOUT_DEFAULT
     **/
    private NIOConfiguration(int minThreads, int maxThreads, int maxConnPool,
                             String bindHostName, String bindPort, int threadsQueueSize, int slowConsumerThroughput,
                             int slowConsumerLatency, int slowConsumerRetries, int slowConsumerReadtimeout,
                             int readSelectorThreads, String watchdogRequestTimeout, String watchdogListeningTimeout,
                             String watchdogIdleConnectionTimeout, long threadPoolIdleTimeout, String socketConnectTimeout,
                             int systemPriorityQueueCapacity, long systemPriorityThreadIdleTimeout,
                             int systemPriorityMinThreads, int systemPriorityMaxThreads,
                             int customQueueCapacity, long customThreadIdleTimeout,
                             int customMinThreads, int customMaxThreads, boolean protocolValidationEnabled) {
        this();
        _minThreads = minThreads;
        _maxThreads = maxThreads;
        _maxConnPool = maxConnPool;
        _bindHostName = bindHostName;
        _bindPort = bindPort;

        _threadsQueueSize = threadsQueueSize;
        _slowConsumerLatency = slowConsumerLatency;
        _slowConsumerThroughput = slowConsumerThroughput;
        _slowConsumerRetries = slowConsumerRetries;
        _slowConsumerReadTimeout = slowConsumerReadtimeout;

        _readSelectorThreads = readSelectorThreads;
        _watchdogRequestTimeout = TimeUnitProperty.getParsedValue(watchdogRequestTimeout);
        _watchdogListeningTimeout = TimeUnitProperty.getParsedValue(watchdogListeningTimeout);
        _watchdogIdleConnectionTimeout = TimeUnitProperty.getParsedValue(watchdogIdleConnectionTimeout);
        _threadPoolIdleTimeout = threadPoolIdleTimeout;
        _socketConnectTimeout = TimeUnitProperty.getParsedValue(socketConnectTimeout);

        _systemPriorityQueueCapacity = systemPriorityQueueCapacity;
        _systemPriorityThreadIdleTimeout = systemPriorityThreadIdleTimeout;
        _systemPriorityMinThreads = systemPriorityMinThreads;
        _systemPriorityMaxThreads = systemPriorityMaxThreads;
        _customQueueCapacity = customQueueCapacity;
        _customThreadIdleTimeout = customThreadIdleTimeout;
        _customMinThreads = customMinThreads;
        _customMaxThreads = customMaxThreads;
        _protocolValidationEnabled = protocolValidationEnabled;
    }

    /**
     * Should not be used, only for externalizable.
     */
    public NIOConfiguration() {
        super();
    }

    @Override
    public ITransportConfig clone() {
        try {
            return (ITransportConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Failed to clone a cloneable class", e);
        }
    }

    @Override
    public String getProtocolAdaptorClass() {
        return PAdapter.class.getName();
    }

    @Override
    public int getMinThreads() {
        return _minThreads;
    }

    public void setMinThreads(int minThreads) {
        _minThreads = minThreads;
    }

    @Override
    public int getMaxThreads() {
        return _maxThreads;
    }

    public void setMaxThreads(int maxThreads) {
        _maxThreads = maxThreads;
    }

    @Override
    public int getThreadsQueueSize() {
        return _threadsQueueSize;
    }

    public void setThreadsQueueSize(int threadsQueueSize) {
        _threadsQueueSize = threadsQueueSize;
    }

    /*
     * @see com.gigaspaces.transport.ITransportConfig#getConnectionPoolSize()
	 */
    public int getConnectionPoolSize() {
        return _maxConnPool;
    }

    public void setMaxConnPool(int maxConnPool) {
        _maxConnPool = maxConnPool;
    }

    public void setBindHost(String bindHostName) {
        _bindHostName = bindHostName;
    }

    public void setBindPort(String bindPort) {
        _bindPort = bindPort;
    }

    public void setBlockingConnection(boolean blockingConnection) {
        _blockingConnection = blockingConnection;
    }

    public int getMaxConnPool() {
        return _maxConnPool;
    }

    public String getBindPort() {
        return _bindPort;
    }

    public String getBindHostName() {
        return _bindHostName;
    }

    /*
     * @see com.gigaspaces.transport.ITransportConfig#getProtocolName()
     */
    final public String getProtocolName() {
        return "NIO";
    }

    public boolean isBlockingConnection() {
        return _blockingConnection;
    }

    /*
     * @see com.gigaspaces.transport.ITransportConfig#getSlowConsumerLatency()
     */
    public int getSlowConsumerLatency() {
        return _slowConsumerLatency;
    }

    public void setSlowConsumerLatency(int slowConsumerLatency) {
        _slowConsumerLatency = slowConsumerLatency;
    }

    /*
     * @see com.gigaspaces.transport.ITransportConfig#getSlowConsumerRetries()
     */
    public int getSlowConsumerRetries() {
        return _slowConsumerRetries;
    }

    public void setSlowConsumerRetries(int slowConsumerRetries) {
        _slowConsumerRetries = slowConsumerRetries;
    }

    /*
     * @see com.gigaspaces.config.lrmi.ITransportConfig#getSlowConsumerReadTimeout()
     */
    public int getSlowConsumerReadTimeout() {
        return _slowConsumerReadTimeout;
    }

    public void setSlowConsumerReadTimeout(int slowConsumerReadTimeout) {
        _slowConsumerReadTimeout = slowConsumerReadTimeout;
    }

    /*
     * @see com.gigaspaces.transport.ITransportConfig#getSlowConsumerThroughput()
     */
    public int getSlowConsumerThroughput() {
        return _slowConsumerThroughput;
    }

    public void setSlowConsumerThroughput(int slowConsumerThroughput) {
        _slowConsumerThroughput = slowConsumerThroughput;
    }

    /**
     * @return Returns the _readSelectorThreads.
     */
    public int getReadSelectorThreads() {
        return _readSelectorThreads;
    }

    @Override
    public int getSystemPriorityQueueCapacity() {
        return _systemPriorityQueueCapacity;
    }

    @Override
    public long getSystemPriorityThreadIdleTimeout() {
        return _systemPriorityThreadIdleTimeout;
    }

    public int getSystemPriorityMinThreads() {
        return _systemPriorityMinThreads;
    }

    public int getSystemPriorityMaxThreads() {
        return _systemPriorityMaxThreads;
    }

    public int getCustomQueueCapacity() {
        return _customQueueCapacity;
    }

    public long getCustomThreadIdleTimeout() {
        return _customThreadIdleTimeout;
    }

    public int getCustomMinThreads() {
        return _customMinThreads;
    }

    public int getCustomMaxThreads() {
        return _customMaxThreads;
    }

    /**
     * @return Returns the _watchdogRequestTimeout.
     */
    public long getWatchdogRequestTimeout() {
        return _watchdogRequestTimeout;
    }

    /**
     * @return Returns the _watchdogListeningTimeout.
     */
    public long getWatchdogListeningTimeout() {
        return _watchdogListeningTimeout;
    }

    /**
     * @return Returns the _watchdogIdleConnectionTimeout.
     */
    public long getWatchdogIdleConnectionTimeout() {
        return _watchdogIdleConnectionTimeout;
    }

    public long getThreadPoolIdleTimeout() {
        return _threadPoolIdleTimeout;
    }

    public long getSocketConnectTimeout() {
        return _socketConnectTimeout;
    }

    public boolean isProtocolValidationEnabled() {
        return _protocolValidationEnabled;
    }


    @Override
    public String toString() {

        final String bindHostNameStr = _bindHostName != null ? _bindHostName
                : "*Undefined* - resolves to the localhost IP address or '-Djava.rmi.server.hostname' system property if exists";

        final StringBuilder sb = new StringBuilder("NIOConfiguration{");
        sb.append("_minThreads=").append(_minThreads);
        sb.append(", _maxThreads=").append(_maxThreads);
        sb.append(", _threadsQueueSize=").append(_threadsQueueSize);
        sb.append(", _maxConnPool=").append(_maxConnPool);
        sb.append(", _bindPort='").append(_bindPort).append('\'');
        sb.append(", _bindHostName='").append(bindHostNameStr).append('\'');
        sb.append(", _blockingConnection=").append(_blockingConnection);
        sb.append(", _slowConsumerLatency=").append(_slowConsumerLatency);
        sb.append(", _slowConsumerThroughput=").append(_slowConsumerThroughput);
        sb.append(", _slowConsumerRetries=").append(_slowConsumerRetries);
        sb.append(", _slowConsumerReadTimeout=").append(_slowConsumerReadTimeout);
        sb.append(", _readSelectorThreads=").append(_readSelectorThreads);
        sb.append(", _watchdogRequestTimeout=").append(_watchdogRequestTimeout);
        sb.append(", _watchdogListeningTimeout=").append(_watchdogListeningTimeout);
        sb.append(", _watchdogIdleConnectionTimeout=").append(_watchdogIdleConnectionTimeout);
        sb.append(", _threadPoolIdleTimeout=").append(_threadPoolIdleTimeout);
        sb.append(", _socketConnectTimeout=").append(_socketConnectTimeout);
        sb.append(", _systemPriorityQueueCapacity=").append(_systemPriorityQueueCapacity);
        sb.append(", _systemPriorityThreadIdleTimeout=").append(_systemPriorityThreadIdleTimeout);
        sb.append(", _systemPriorityMinThreads=").append(_systemPriorityMinThreads);
        sb.append(", _systemPriorityMaxThreads=").append(_systemPriorityMaxThreads);
        sb.append(", _customQueueCapacity=").append(_customQueueCapacity);
        sb.append(", _customThreadIdleTimeout=").append(_customThreadIdleTimeout);
        sb.append(", _customMinThreads=").append(_customMinThreads);
        sb.append(", _customMaxThreads=").append(_customMaxThreads);
        sb.append(", _protocolValidationEnabled=").append(_protocolValidationEnabled);
        sb.append('}');
        return sb.toString();
    }
}
