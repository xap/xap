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

package com.gigaspaces.cluster.activeelection.core;

import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.cluster.startup.GigaSpacesFaultDetectionHandler;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * This class provides a configuration object of {@link ActiveElectionManager}.
 *
 * @author Igor Goldenberg
 * @see ActiveElectionManager
 * @since 6.0
 **/
@com.gigaspaces.api.InternalApi
public class ActiveElectionConfig implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    public final static int DEFAULT_RETRY_COUNT = 60;
    public final static long DEFAULT_YIELD_TIME = 1000; // ms
    public final static long RESOLUTION_TIMEOUT_DEFAULT = 10 * 1000; //ms

    private int _retryConnection = DEFAULT_RETRY_COUNT;
    private long _yieldTime = DEFAULT_YIELD_TIME;
    private long _resolutionTimeout = RESOLUTION_TIMEOUT_DEFAULT;

    /**
     * fault detector configuration
     */
    public final static long DEFAULT_FAULTDETECTOR_INVOCATION_DELAY = 1000; // ms
    public final static int DEFAULT_FAULTDETECTOR_RETRY_COUNT = 3;
    public final static long DEFAULT_FAULTDETECTOR_RETRY_TIMEOUT = 100; // ms

    private long _faultDetectorInvocationDelay = DEFAULT_FAULTDETECTOR_INVOCATION_DELAY;
    private int _faultDetectorRetryCount = DEFAULT_FAULTDETECTOR_RETRY_COUNT;
    private long _faultDetectorRetryTimeout = DEFAULT_FAULTDETECTOR_RETRY_TIMEOUT;

    private interface BitMap {
        byte RETRY_CONNECTION = 1 << 0;
        byte YIELD_TIME = 1 << 1;
        byte FAULT_DETECTOR_INV_DELAY = 1 << 2;
        byte FAULT_DETECTOR_RETRY_COUNT = 1 << 3;
        byte FAULT_DETECTOR_RETRY_TIMEOUT = 1 << 4;
        byte RESOLUTION_TIMEOUT = 1 << 5;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        byte flags = 0;
        if (_retryConnection != DEFAULT_RETRY_COUNT) {
            flags |= BitMap.RETRY_CONNECTION;
        }
        if (_yieldTime != DEFAULT_YIELD_TIME) {
            flags |= BitMap.YIELD_TIME;
        }
        if (_faultDetectorInvocationDelay != DEFAULT_FAULTDETECTOR_INVOCATION_DELAY) {
            flags |= BitMap.FAULT_DETECTOR_INV_DELAY;
        }
        if (_faultDetectorRetryCount != DEFAULT_FAULTDETECTOR_RETRY_COUNT) {
            flags |= BitMap.FAULT_DETECTOR_RETRY_COUNT;
        }
        if (_faultDetectorRetryTimeout != DEFAULT_FAULTDETECTOR_RETRY_TIMEOUT) {
            flags |= BitMap.FAULT_DETECTOR_RETRY_TIMEOUT;
        }
        if (_resolutionTimeout != RESOLUTION_TIMEOUT_DEFAULT) {
            flags |= BitMap.RESOLUTION_TIMEOUT;
        }
        out.writeByte(flags);
        if (_faultDetectorRetryCount != DEFAULT_FAULTDETECTOR_RETRY_COUNT) {
            flags |= BitMap.FAULT_DETECTOR_RETRY_COUNT;
        }
        if (_retryConnection != DEFAULT_RETRY_COUNT) {
            out.writeInt(_retryConnection);
        }
        if (_yieldTime != DEFAULT_YIELD_TIME) {
            out.writeLong(_yieldTime);
        }
        if (_faultDetectorInvocationDelay != DEFAULT_FAULTDETECTOR_INVOCATION_DELAY) {
            out.writeLong(_faultDetectorInvocationDelay);
        }
        if (_faultDetectorRetryCount != DEFAULT_FAULTDETECTOR_RETRY_COUNT) {
            out.writeInt(_faultDetectorRetryCount);
        }
        if (_faultDetectorRetryTimeout != DEFAULT_FAULTDETECTOR_RETRY_TIMEOUT) {
            out.writeLong(_faultDetectorRetryTimeout);
        }
        if (_resolutionTimeout != RESOLUTION_TIMEOUT_DEFAULT)
            out.writeLong(_resolutionTimeout);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte flags = in.readByte();
        if ((flags & BitMap.RETRY_CONNECTION) != 0) {
            _retryConnection = in.readInt();
        } else {
            _retryConnection = DEFAULT_RETRY_COUNT;
        }
        if ((flags & BitMap.YIELD_TIME) != 0) {
            _yieldTime = in.readLong();
        } else {
            _yieldTime = DEFAULT_YIELD_TIME;
        }
        if ((flags & BitMap.FAULT_DETECTOR_INV_DELAY) != 0) {
            _faultDetectorInvocationDelay = in.readLong();
        } else {
            _faultDetectorInvocationDelay = DEFAULT_FAULTDETECTOR_INVOCATION_DELAY;
        }
        if ((flags & BitMap.FAULT_DETECTOR_RETRY_COUNT) != 0) {
            _faultDetectorRetryCount = in.readInt();
        } else {
            _faultDetectorRetryCount = DEFAULT_FAULTDETECTOR_RETRY_COUNT;
        }
        if ((flags & BitMap.FAULT_DETECTOR_RETRY_TIMEOUT) != 0) {
            _faultDetectorRetryTimeout = in.readLong();
        } else {
            _faultDetectorRetryTimeout = DEFAULT_FAULTDETECTOR_RETRY_TIMEOUT;
        }
        if ((flags & BitMap.RESOLUTION_TIMEOUT) != 0) {
            _resolutionTimeout = in.readLong();
        } else {
            _resolutionTimeout = RESOLUTION_TIMEOUT_DEFAULT;
        }
    }

    /**
     * @return number of retry connections with {@link com.gigaspaces.internal.naming.INamingService},
     * default naming service is LookupService.
     */
    public int getRetryConnection() {
        return _retryConnection;
    }

    /**
     * @return yield time between every election phase.
     */
    public long getYieldTime() {
        return _yieldTime;
    }

    /**
     * Set number of retry connections with {@link com.gigaspaces.internal.naming.INamingService},
     * default naming service is LookupService.
     *
     * @param retryConnection number of retry connections.
     */
    public void setRetryConnection(int retryConnection) {
        _retryConnection = retryConnection;
    }

    /**
     * Time to yield to other participants between every election phase(total 3) before aquire
     * Primary or Backup state.
     *
     * @param yieldTime time in milliseconds.
     */
    public void setYieldTime(long yieldTime) {
        _yieldTime = yieldTime;
    }

    /**
     * @return The amount of time in milliseconds to wait between {@link
     * com.j_spaces.core.IJSpace#ping()} method invocations.
     **/
    public long getFaultDetectorInvocationDelay() {
        return _faultDetectorInvocationDelay;
    }

    /**
     * Set amount of time in milliseconds to wait between {@link com.j_spaces.core.IJSpace#ping()}
     * method invocations.
     *
     * @param detectorInvocationDelay delay time in ms.
     **/
    public void setFaultDetectorInvocationDelay(long detectorInvocationDelay) {
        _faultDetectorInvocationDelay = detectorInvocationDelay;
    }

    /**
     * @return the number of times to retry connecting to the service when invoking the {@link
     * com.j_spaces.core.IJSpace#ping()} method. If the service cannot be reached within the retry
     * count specified the service will be determined to be unreachable.
     */
    public int getFaultDetectorRetryCount() {
        return _faultDetectorRetryCount;
    }

    /**
     * Set number of times to retry connecting to the service when invoking the {@link
     * com.j_spaces.core.IJSpace#ping()} method. If the service cannot be reached within the retry
     * count specified the service will be determined to be unreachable.
     *
     * @param detectorRetryCount connection retry count.
     */
    public void setFaultDetectorRetryCount(int detectorRetryCount) {
        _faultDetectorRetryCount = detectorRetryCount;
    }

    /**
     * @return How long to wait between retries (in milliseconds). This value will be used between
     * retry attempts, waiting the specified amount of time to retry.
     */
    public long getFaultDetectorRetryTimeout() {
        return _faultDetectorRetryTimeout;
    }

    /**
     * Set how long to wait between retries (in milliseconds). This value will be used between retry
     * attempts, waiting the specified amount of time to retry.
     *
     * @param detectorRetryTimeout retry timeout in ms.
     */
    public void setFaultDetectorRetryTimeout(long detectorRetryTimeout) {
        _faultDetectorRetryTimeout = detectorRetryTimeout;
    }

    public long getResolutionTimeout() {
        return _resolutionTimeout;
    }

    public void setResolutionTimeout(long resolutionTimeout) {
        _resolutionTimeout = resolutionTimeout;
    }


    /**
     * Get fault detection configuration
     **/
    public String[] getFDHConfig() {

        String className = GigaSpacesFaultDetectionHandler.class.getName();
        String[] fdhConfig = new String[4];
        fdhConfig[0] = "-";
        fdhConfig[1] = className + ".retryCount=" + getFaultDetectorRetryCount();
        fdhConfig[2] = className + ".invocationDelay=" + getFaultDetectorInvocationDelay();
        fdhConfig[3] = className + ".retryTimeout=" + getFaultDetectorRetryTimeout();

        return fdhConfig;
    }

    @Override
    public String toString() {
        StringBuilder sb =
                new StringBuilder("ActiveElectionConfig: ");
        sb.append("RetryConnection=" + getRetryConnection());
        sb.append(", YieldTime=" + getYieldTime());
        sb.append(", FaultDetectorInvocationDelay=" + getFaultDetectorInvocationDelay());
        sb.append(", FaultDetectorRetryCount=" + getFaultDetectorRetryCount());
        sb.append(", FaultDetectorRetryTimeout=" + getFaultDetectorRetryTimeout());
        sb.append(", ResolutionTimeout=" + getResolutionTimeout());

        return sb.toString();
    }
}