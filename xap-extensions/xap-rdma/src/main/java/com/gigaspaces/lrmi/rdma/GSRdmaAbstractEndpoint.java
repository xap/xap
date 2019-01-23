package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;

public abstract class GSRdmaAbstractEndpoint extends RdmaActiveEndpoint {

    public GSRdmaAbstractEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
        super(group, idPriv, serverSide);
    }
}
