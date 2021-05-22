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
import com.gigaspaces.lrmi.LRMIServiceClientMonitoringDetails;
import com.gigaspaces.lrmi.LRMIServiceClientMonitoringId;
import com.gigaspaces.lrmi.LRMIServiceMonitoringDetails;
import com.gigaspaces.lrmi.nio.ChannelEntry;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Tracking details of a single hosted service
 *
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class LRMIServiceMonitoringDetailsImpl implements SmartExternalizable, LRMIServiceMonitoringDetails {

    private static final long serialVersionUID = 1L;
    private String _serviceDetails;
    private String _serviceClassLoaderDetails;
    private long _remoteObjID;
    private String _connectionUrl;
    private Map<LRMIServiceClientMonitoringId, LRMIServiceClientMonitoringDetails> _clientsMonitoringDetails;

    public LRMIServiceMonitoringDetailsImpl() {
    }

    public LRMIServiceMonitoringDetailsImpl(String serviceDetails, String serviceClassLoaderDetails, long remoteObjID, String connectionUrl) {
        _serviceDetails = serviceDetails;
        _serviceClassLoaderDetails = serviceClassLoaderDetails;
        _remoteObjID = remoteObjID;
        _connectionUrl = connectionUrl;
        _clientsMonitoringDetails = new HashMap<LRMIServiceClientMonitoringId, LRMIServiceClientMonitoringDetails>();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _serviceDetails);
        IOUtils.writeString(out, _serviceClassLoaderDetails);
        IOUtils.writeString(out, _connectionUrl);
        out.writeLong(_remoteObjID);
        IOUtils.writeObject(out, _clientsMonitoringDetails);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _serviceDetails = IOUtils.readString(in);
        _serviceClassLoaderDetails = IOUtils.readString(in);
        _connectionUrl = IOUtils.readString(in);
        _remoteObjID = in.readLong();
        _clientsMonitoringDetails = IOUtils.readObject(in);
    }

    public void addChannelDetails(ChannelEntry channelEntry) {
        Socket socket = channelEntry.getSocketChannel().socket();
        if (socket == null)
            return;

        InetAddress remoteInetAddress = socket.getInetAddress();
        if (remoteInetAddress == null)
            return;

        long sourcePid = channelEntry.getSourcePid();

        LRMIServiceClientMonitoringIdImpl clientId = new LRMIServiceClientMonitoringIdImpl(remoteInetAddress, sourcePid);

        LRMIServiceClientMonitoringDetailsImpl clientMonitoringDetails = (LRMIServiceClientMonitoringDetailsImpl) _clientsMonitoringDetails.get(clientId);
        if (clientMonitoringDetails == null) {
            clientMonitoringDetails = new LRMIServiceClientMonitoringDetailsImpl(channelEntry.getSourcePlatformLogicalVersion());
            _clientsMonitoringDetails.put(clientId, clientMonitoringDetails);
        }

        clientMonitoringDetails.addChannelDetails(channelEntry);
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.lrmi.nio.LRMIServiceMonitoringDetails#getServiceDetails()
	 */
    @Override
    public String getServiceDetails() {
        return _serviceDetails;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.lrmi.nio.LRMIServiceMonitoringDetails#getServiceClassLoaderDetails()
	 */
    @Override
    public String getServiceClassLoaderDetails() {
        return _serviceClassLoaderDetails;
    }

    /* (non-Javadoc)
	 * @see com.gigaspaces.lrmi.nio.LRMIServiceMonitoringDetails#getRemoteObjID()
	 */
    @Override
    public long getRemoteObjID() {
        return _remoteObjID;
    }

    @Override
    public String getConnectionUrl() {
        return _connectionUrl;
    }

    /* (non-Javadoc)
	 * @see com.gigaspaces.lrmi.nio.LRMIServiceMonitoringDetails#getClientsTrackingDetails()
	 */
    @Override
    public Map<LRMIServiceClientMonitoringId, LRMIServiceClientMonitoringDetails> getClientsTrackingDetails() {
        return _clientsMonitoringDetails;
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("LRMIServiceTrackingDetails (ServiceDetails=");
        builder.append(getServiceDetails());
        builder.append(" ServiceClassLoaderDetails=");
        builder.append(getServiceClassLoaderDetails());
        builder.append(" RemoteObjectId=");
        builder.append(getRemoteObjID());
        builder.append(StringUtils.NEW_LINE);
        builder.append(" ConnectionUrl=");
        builder.append(getConnectionUrl());
        builder.append(")");
        builder.append(StringUtils.NEW_LINE);

        //Sort display from the method which is correlated to must generated traffic
        List<Map.Entry<LRMIServiceClientMonitoringId, LRMIServiceClientMonitoringDetails>> list =
                new LinkedList<Map.Entry<LRMIServiceClientMonitoringId, LRMIServiceClientMonitoringDetails>>(_clientsMonitoringDetails.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<LRMIServiceClientMonitoringId, LRMIServiceClientMonitoringDetails>>() {
            public int compare(Map.Entry<LRMIServiceClientMonitoringId, LRMIServiceClientMonitoringDetails> o1, Map.Entry<LRMIServiceClientMonitoringId, LRMIServiceClientMonitoringDetails> o2) {
                long o1Traffic = o1.getValue().getTotalGeneratedTraffic() + o1.getValue().getTotalReceivedTraffic();
                long o2Traffic = o2.getValue().getTotalGeneratedTraffic() + o2.getValue().getTotalReceivedTraffic();
                if (o1Traffic == o2Traffic)
                    return 0;
                return o1Traffic < o2Traffic ? 1 : -1;
            }
        });

        for (Entry<LRMIServiceClientMonitoringId, LRMIServiceClientMonitoringDetails> clientTrackingDetails : list) {
            builder.append("\t");
            builder.append(clientTrackingDetails.getKey());
            builder.append(":");
            builder.append(StringUtils.NEW_LINE);
            builder.append("\t\t");
            builder.append(clientTrackingDetails.getValue());
            builder.append(StringUtils.NEW_LINE);
        }
        return builder.toString();

    }
}
