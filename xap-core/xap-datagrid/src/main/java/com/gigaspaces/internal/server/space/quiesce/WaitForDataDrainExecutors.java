package com.gigaspaces.internal.server.space.quiesce;

import com.gigaspaces.internal.extension.WaitForDataDrainRequest;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.executors.SpaceActionExecutor;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

import java.util.concurrent.TimeoutException;

public class WaitForDataDrainExecutors extends SpaceActionExecutor {

    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        final WaitForDataDrainRequest drainRequest = (WaitForDataDrainRequest) spaceRequestInfo;
        final WaitForDrainPartitionResponse response = new WaitForDrainPartitionResponse(space.getPartitionIdOneBased());
        try {
            space.waitForDrain(drainRequest.getTimeout(), drainRequest.getMinTimeToWait(), drainRequest.isDemote(), null);
            response.setSuccessful(true);
        } catch (TimeoutException e) {
            response.setException(e);
        }

        return response;
    }
}
