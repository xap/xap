package com.gigaspaces.lrmi.rdma;

import com.gigaspaces.config.lrmi.nio.NIOConfiguration;
import com.gigaspaces.internal.lrmi.LRMIInboundMonitoringDetailsImpl;
import com.gigaspaces.lrmi.AbstractPivot;
import com.gigaspaces.lrmi.nio.PAdapter;
import com.gigaspaces.management.transport.ITransportConnection;

import java.net.InetSocketAddress;
import java.util.List;

public class RdmaPivot extends AbstractPivot {

    public RdmaPivot(NIOConfiguration nioConfig, PAdapter pAdapter) {
        super();
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public String getHostName() {
        return null;
    }

    @Override
    public InetSocketAddress getServerBindInetSocketAddress() {
        return null;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public List<ITransportConnection> getRemoteObjectConnectionsList(long objectId) {
        return null;
    }

    @Override
    public int countRemoteObjectConnections(long objectId) {
        return 0;
    }

    @Override
    public LRMIInboundMonitoringDetailsImpl getMonitoringDetails() {
        return null;
    }

    @Override
    public void unexport(long objectId) {

    }
}
