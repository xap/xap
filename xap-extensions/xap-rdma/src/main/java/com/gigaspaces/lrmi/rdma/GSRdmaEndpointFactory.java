package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

public class GSRdmaEndpointFactory implements RdmaEndpointFactory<GSRdmaAbstractEndpoint> {

    private final RdmaActiveEndpointGroup<GSRdmaAbstractEndpoint> endpointGroup;
    private RdmaResourceFactory factory;
    private Function<ByteBuffer, Object> deserialize;

    public GSRdmaEndpointFactory(RdmaResourceFactory factory, Function<ByteBuffer, Object> deserialize) throws IOException {
        //create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
        endpointGroup = new RdmaActiveEndpointGroup<>(1000, false, 128, 4, 128);
        endpointGroup.init(this);
        this.factory = factory;
        this.deserialize = deserialize;
    }

    public GSRdmaAbstractEndpoint create() throws IOException {
        return endpointGroup.createEndpoint();
    }

    @Override
    public GSRdmaAbstractEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        if (serverSide) {
            return new GSRdmaServerEndpoint(endpointGroup, id, factory);
        } else {
            return new GSRdmaClientEndpoint(endpointGroup, id, factory, deserialize);
        }
    }

    public RdmaActiveEndpointGroup<GSRdmaAbstractEndpoint> getEndpointGroup() {
        return endpointGroup;
    }

    public void close() throws IOException, InterruptedException {
        endpointGroup.close();
    }
}
