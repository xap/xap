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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.ConnectionResource;
import com.gigaspaces.lrmi.LRMIMethodMonitoringDetails;
import com.gigaspaces.lrmi.LRMIProxyMonitoringDetails;
import com.gigaspaces.lrmi.LRMIUtilities;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class LRMIProxyMonitoringDetailsImpl implements LRMIProxyMonitoringDetails, SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private long _totalReceivedTraffic;
    private long _totalGeneratedTraffic;
    private String _connectionURL;
    private String _serviceDetails;
    private Map<String, LRMIMethodMonitoringDetails> _monitoringDetails;
    private PlatformLogicalVersion _serviceVersion;

    public LRMIProxyMonitoringDetailsImpl() {
    }

    public LRMIProxyMonitoringDetailsImpl(String connectionURL, String serviceDetails, PlatformLogicalVersion serviceVersion) {
        _connectionURL = connectionURL;
        _serviceDetails = serviceDetails;
        _serviceVersion = serviceVersion;
    }

    @Override
    public long getTotalReceivedTraffic() {
        return _totalReceivedTraffic;
    }

    @Override
    public long getTotalGeneratedTraffic() {
        return _totalGeneratedTraffic;
    }

    @Override
    public String getConnectionUrl() {
        return _connectionURL;
    }

    @Override
    public String getServiceDetails() {
        return _serviceDetails;
    }

    @Override
    public PlatformLogicalVersion getServiceVersion() {
        return _serviceVersion;
    }

    @Override
    public Map<String, LRMIMethodMonitoringDetails> getTrackingDetails() {
        return _monitoringDetails;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_totalReceivedTraffic);
        out.writeLong(_totalGeneratedTraffic);
        IOUtils.writeString(out, _connectionURL);
        IOUtils.writeString(out, _serviceDetails);
        IOUtils.writeObject(out, _serviceVersion);
        IOUtils.writeObject(out, _monitoringDetails);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _totalReceivedTraffic = in.readLong();
        _totalGeneratedTraffic = in.readLong();
        _connectionURL = IOUtils.readString(in);
        _serviceDetails = IOUtils.readString(in);
        _serviceVersion = IOUtils.readObject(in);
        _monitoringDetails = IOUtils.readObject(in);
    }

    public void addConnectionResource(ConnectionResource resource) {
        _totalReceivedTraffic += resource.getReceivedTraffic();
        _totalGeneratedTraffic += resource.getGeneratedTraffic();
        if (_monitoringDetails == null)
            _monitoringDetails = new HashMap<String, LRMIMethodMonitoringDetails>();
        resource.getMonitoringModule().addMonitoringActivity(_monitoringDetails);
    }

    @Override
    public long getServicePid() {
        return ConnectionUrlDescriptor.fromUrl(_connectionURL).getPid();
    }

    @Override
    public String getServiceHostname() {
        return ConnectionUrlDescriptor.fromUrl(_connectionURL).getHostname();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("LRMIProxyMonitoringDetails (ServiceDetails=" + getServiceDetails() +
                " TotalReceivedTraffic=");
        builder.append(LRMIUtilities.getTrafficString(getTotalReceivedTraffic()));
        builder.append(" TotalGeneratedTraffic=");
        builder.append(LRMIUtilities.getTrafficString(getTotalGeneratedTraffic()));
        builder.append(" TotalTraffic=");
        builder.append(LRMIUtilities.getTrafficString(getTotalReceivedTraffic() + getTotalGeneratedTraffic()));
        builder.append(StringUtils.NEW_LINE);
        builder.append(" ConnectionUrl=");
        builder.append(getConnectionUrl());
        builder.append(" ServiceVersion=");
        builder.append(getServiceVersion());
        builder.append(")");
        if (_monitoringDetails != null) {
            //Sort display from the method which is correlated to must generated traffic
            List<Map.Entry<String, LRMIMethodMonitoringDetails>> list =
                    new LinkedList<Map.Entry<String, LRMIMethodMonitoringDetails>>(_monitoringDetails.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, LRMIMethodMonitoringDetails>>() {
                public int compare(Map.Entry<String, LRMIMethodMonitoringDetails> o1, Map.Entry<String, LRMIMethodMonitoringDetails> o2) {
                    long o1Traffic = o1.getValue().getGeneratedTraffic() + o1.getValue().getReceivedTraffic();
                    long o2Traffic = o2.getValue().getGeneratedTraffic() + o2.getValue().getReceivedTraffic();
                    if (o1Traffic == o2Traffic)
                        return 0;
                    return o1Traffic < o2Traffic ? 1 : -1;
                }
            });
            builder.append(StringUtils.NEW_LINE);
            for (Entry<String, LRMIMethodMonitoringDetails> entry : list) {
                builder.append("\t\t");
                builder.append(entry.getKey());
                builder.append(": ");
                builder.append(entry.getValue());
                builder.append(StringUtils.NEW_LINE);
            }
        }
        return builder.toString();
    }

}

