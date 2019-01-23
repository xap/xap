package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;

public class GSRdmaEndpointFactory implements RdmaEndpointFactory<GSRdmaAbstractEndpoint> {

    private final RdmaActiveEndpointGroup<GSRdmaAbstractEndpoint> endpointGroup;

    public GSRdmaEndpointFactory() throws IOException {
        //create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 4, 128);
        endpointGroup.init(this);
    }

    public GSRdmaAbstractEndpoint create() throws IOException {
        return endpointGroup.createEndpoint();
    }

    @Override
    public GSRdmaAbstractEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        if (serverSide) {
            return new GSRdmaServerEndpoint(endpointGroup, id);
        } else {
            return new GSRdmaClientEndpoint(endpointGroup, id);
        }
    }

    public RdmaActiveEndpointGroup<GSRdmaAbstractEndpoint> getEndpointGroup() {
        return endpointGroup;
    }

    public void close() throws IOException, InterruptedException {
        endpointGroup.close();
    }
}
