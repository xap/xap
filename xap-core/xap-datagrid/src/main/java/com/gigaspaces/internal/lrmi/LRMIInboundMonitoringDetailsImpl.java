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
import com.gigaspaces.lrmi.LRMIServiceMonitoringDetails;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class LRMIInboundMonitoringDetailsImpl
        implements LRMIInboundMonitoringDetails, SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private LRMIServiceMonitoringDetailsImpl[] _serviceMonitoringDetails;

    public LRMIInboundMonitoringDetailsImpl() {
    }

    public LRMIInboundMonitoringDetailsImpl(LRMIServiceMonitoringDetailsImpl[] serviceMonitoringDetails) {
        _serviceMonitoringDetails = serviceMonitoringDetails;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_serviceMonitoringDetails);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _serviceMonitoringDetails = (LRMIServiceMonitoringDetailsImpl[]) in.readObject();
    }

    @Override
    public LRMIServiceMonitoringDetails[] getServicesMonitoringDetails() {
        return _serviceMonitoringDetails;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("LRMI Inbound monitoring details:");
        builder.append(StringUtils.NEW_LINE);
        for (LRMIServiceMonitoringDetails details : _serviceMonitoringDetails) {
            builder.append(details);
            builder.append(StringUtils.NEW_LINE);
        }
        return builder.toString();
    }

}
