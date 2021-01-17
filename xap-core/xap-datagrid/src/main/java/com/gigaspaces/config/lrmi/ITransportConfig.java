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

package com.gigaspaces.config.lrmi;

import java.io.Externalizable;

/**
 * This interface defines the underline communication transport protocol configuration.
 *
 * @author Igor Goldenberg
 * @see GenericExporter
 * @since 5.2
 **/
public interface ITransportConfig extends Externalizable {
    /**
     * @return the protocol adaptor name of underlying transport protocol
     */
    String getProtocolName();

    /**
     * @return the connection socket pool size of protocol adaptor
     */
    int getConnectionPoolSize();

    /**
     * @return the protocol adaptor classname
     */
    String getProtocolAdaptorClass();

    /**
     * @return is blocking connection
     */
    boolean isBlockingConnection();

    /**
     * @return Slow Consumer Throughput
     */
    int getSlowConsumerThroughput();

    /**
     * @return Slow Consumer Latency
     */
    int getSlowConsumerLatency();

    /**
     * @return Slow Consumer Retries
     */
    int getSlowConsumerRetries();

    /**
     * @return LRMI number of read selector threads.
     */
    int getReadSelectorThreads();

    /**
     * @return LRMI number of system read selector threads
     */
    int getSystemPriorityQueueCapacity();

    long getSystemPriorityThreadIdleTimeout();

    /**
     * @return The watchdog Request timeout
     */
    long getWatchdogRequestTimeout();

    /**
     * @return The watchdog Listening timeout
     */
    long getWatchdogListeningTimeout();

    /**
     * @return The watchdog connection idle time
     */
    long getWatchdogIdleConnectionTimeout();

    /**
     * @return LRMI ThreadPool idle timeout
     */
    long getThreadPoolIdleTimeout();

    /**
     * @return The timeout on lrmi socket connect
     */
    long getSocketConnectTimeout();

    /**
     * @return The minimal number of transport threads
     */
    int getMinThreads();

    /**
     * @return The maximal number of transport threads
     */
    int getMaxThreads();

    /**
     * @return The threads queue size
     */
    int getThreadsQueueSize();

    ITransportConfig clone();
}