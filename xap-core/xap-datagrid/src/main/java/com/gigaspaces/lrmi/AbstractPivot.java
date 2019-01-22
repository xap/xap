package com.gigaspaces.lrmi;

import com.gigaspaces.internal.lrmi.LRMIInboundMonitoringDetailsImpl;
import com.gigaspaces.management.transport.ITransportConnection;

import java.net.InetSocketAddress;
import java.util.List;

public abstract class AbstractPivot {

    public abstract int getPort();

    public abstract String getHostName();

    public abstract InetSocketAddress getServerBindInetSocketAddress();

    public abstract void shutdown();

    public abstract List<ITransportConnection> getRemoteObjectConnectionsList(long objectId);

    public abstract int countRemoteObjectConnections(long objectId);

    public abstract LRMIInboundMonitoringDetailsImpl getMonitoringDetails();

    public abstract void unexport(long objectId);
}
