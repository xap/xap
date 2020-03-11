package com.gigaspaces.internal.client.spaceproxy.actions;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.operations.RenewIteratorLeaseSpaceOperationRequest;

import java.rmi.RemoteException;
import java.util.UUID;

public class SpaceProxyImplRenewSpaceIteratorLeaseAction extends RenewSpaceIteratorLeaseProxyAction{
    @Override
    public void renewSpaceIteratorLease(ISpaceProxy spaceProxy, UUID uuid) throws RemoteException, InterruptedException {
        RenewIteratorLeaseSpaceOperationRequest renewIteratorLeaseSpaceOperationRequest = new RenewIteratorLeaseSpaceOperationRequest(uuid);
        spaceProxy.getDirectProxy().getProxyRouter().execute(renewIteratorLeaseSpaceOperationRequest);
        renewIteratorLeaseSpaceOperationRequest.getFinalResult().processExecutionException();
    }
}
