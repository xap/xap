package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;

public class GSRdmaEndpointFactory implements RdmaEndpointFactory<GSRdmaClientEndpoint> {

    private final RdmaActiveEndpointGroup<GSRdmaClientEndpoint> endpointGroup;

    public GSRdmaEndpointFactory() throws IOException {
        //create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 4, 128);
        endpointGroup.init(this);
    }

    public GSRdmaClientEndpoint create() throws IOException {
        return endpointGroup.createEndpoint();
    }

    @Override
    public GSRdmaClientEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new GSRdmaClientEndpoint(endpointGroup, id, serverSide);
    }

    public void close() throws IOException, InterruptedException {
        endpointGroup.close();
    }
}
