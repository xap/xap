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

package com.gigaspaces.internal.lrmi;

import com.gigaspaces.lrmi.LRMIMethodMonitoringDetails;
import com.gigaspaces.lrmi.LRMIUtilities;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Tracking details of a specific method
 *
 * @author eitany
 * @see LRMIServiceClientMonitoringDetailsImpl
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class LRMIMethodMonitoringDetailsImpl implements SmartExternalizable, LRMIMethodMonitoringDetails {

    private static final long serialVersionUID = 1L;

    private long _generatedTraffic;
    private long _receivedTraffic;
    private long _invocationCount;

    public LRMIMethodMonitoringDetailsImpl() {
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_generatedTraffic);
        out.writeLong(_receivedTraffic);
        out.writeLong(_invocationCount);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _generatedTraffic = in.readLong();
        _receivedTraffic = in.readLong();
        _invocationCount = in.readLong();
    }

    public void addTrackingDetails(long thisInvocationGeneratedTraffic, long thisInvocationReceivedTraffic) {
        _invocationCount++;
        _generatedTraffic += thisInvocationGeneratedTraffic;
        _receivedTraffic += thisInvocationReceivedTraffic;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.lrmi.nio.LRMIMethodMonitoringDetails#getInvocationCount()
	 */
    @Override
    public long getInvocationCount() {
        return _invocationCount;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.lrmi.nio.LRMIMethodMonitoringDetails#getGeneratedTraffic()
	 */
    @Override
    public long getGeneratedTraffic() {
        return _generatedTraffic;
    }

    /* (non-Javadoc)
	 * @see com.gigaspaces.lrmi.nio.LRMIMethodMonitoringDetails#getReceivedTraffic()
	 */
    @Override
    public long getReceivedTraffic() {
        return _receivedTraffic;
    }

    public void addTrackingDetails(LRMIMethodMonitoringDetails methodTrackingDetails) {
        _invocationCount += methodTrackingDetails.getInvocationCount();
        _generatedTraffic += methodTrackingDetails.getGeneratedTraffic();
        _receivedTraffic += methodTrackingDetails.getReceivedTraffic();
    }

    @Override
    public String toString() {
        return "invoked count=" + _invocationCount + " received traffic="
                + LRMIUtilities.getTrafficString(_receivedTraffic) + " generated traffic="
                + LRMIUtilities.getTrafficString(_generatedTraffic) + " total traffic=" +
                LRMIUtilities.getTrafficString(_receivedTraffic + _generatedTraffic);
    }

}
