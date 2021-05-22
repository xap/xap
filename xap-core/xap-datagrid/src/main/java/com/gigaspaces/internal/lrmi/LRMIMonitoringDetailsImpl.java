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

import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.lrmi.LRMIInboundMonitoringDetails;
import com.gigaspaces.lrmi.LRMIMonitoringDetails;
import com.gigaspaces.lrmi.LRMIOutboundMonitoringDetails;
import com.gigaspaces.lrmi.nio.info.NIOInfoHelper;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.start.SystemInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class LRMIMonitoringDetailsImpl
        implements LRMIMonitoringDetails, SmartExternalizable {

    private static final long serialVersionUID = 1L;
    private LRMIInboundMonitoringDetailsImpl _inboundMonitoringDetails;
    private LRMIOutboundMonitoringDetailsImpl _outboundMonitoringDetails;

    public LRMIMonitoringDetailsImpl() {
    }

    public LRMIMonitoringDetailsImpl(LRMIInboundMonitoringDetailsImpl inboundMonitoringDetails, LRMIOutboundMonitoringDetailsImpl outboundMonitoringDetails) {
        _inboundMonitoringDetails = inboundMonitoringDetails;
        _outboundMonitoringDetails = outboundMonitoringDetails;
    }

    @Override
    public LRMIOutboundMonitoringDetails getOutboundMonitoringDetails() {
        return _outboundMonitoringDetails;
    }

    @Override
    public LRMIInboundMonitoringDetails getInboundMonitoringDetails() {
        return _inboundMonitoringDetails;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_outboundMonitoringDetails);
        out.writeObject(_inboundMonitoringDetails);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _outboundMonitoringDetails = (LRMIOutboundMonitoringDetailsImpl) in.readObject();
        _inboundMonitoringDetails = (LRMIInboundMonitoringDetailsImpl) in.readObject();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("LRMI Monitoring Details pid[" + SystemInfo.singleton().os().processId() + "] ");
        builder.append(NIOInfoHelper.getDetails());
        builder.append(":" + StringUtils.NEW_LINE);
        builder.append(_inboundMonitoringDetails);
        builder.append(StringUtils.NEW_LINE);
        builder.append(_outboundMonitoringDetails);
        return builder.toString();
    }

}
