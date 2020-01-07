package com.gigaspaces.internal.client.spaceproxy.actions;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.operations.CloseIteratorSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.CloseIteratorSpaceOperationResult;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.UUID;

public class SpaceProxyImplCloseSpaceIteratorAction extends CloseSpaceIteratorProxyAction{
    @Override
    public void closeSpaceIterator(ISpaceProxy spaceProxy, UUID uuid) throws RemoteException, InterruptedException {
        CloseIteratorSpaceOperationRequest closeIteratorSpaceOperationRequest = new CloseIteratorSpaceOperationRequest(uuid);
        spaceProxy.getDirectProxy().getProxyRouter().execute(closeIteratorSpaceOperationRequest);
        closeIteratorSpaceOperationRequest.getFinalResult().processExecutionException();
    }
}
