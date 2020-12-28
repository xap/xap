package com.gigaspaces.internal.space.requests;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.space.responses.BroadcastTableSpaceResponseInfo;

import java.util.List;

/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public abstract class BroadcastTableSpaceRequestInfo extends AbstractSpaceRequestInfo {
    private static final long serialVersionUID = 1L;
    public enum Action {
        PUSH_ENTRY (0),
        PUSH_ENTRIES (1),
        PULL_ENTRIES (2);
        public final byte value;
        Action(int value) {
            this.value = (byte)  value;
        }
    }

    public BroadcastTableSpaceRequestInfo() {
        super();
    }

    public abstract Action getAction();

    public BroadcastTableSpaceResponseInfo reduce(List<AsyncResult<BroadcastTableSpaceResponseInfo>> asyncResults) throws Exception {
        BroadcastTableSpaceResponseInfo result = new BroadcastTableSpaceResponseInfo();
        for (AsyncResult<BroadcastTableSpaceResponseInfo> asyncResult : asyncResults){
            if(asyncResult.getException() != null) {
                throw asyncResult.getException();
            }
            BroadcastTableSpaceResponseInfo responseInfo = asyncResult.getResult();
            result.getExceptionMap().putAll(responseInfo.getExceptionMap());
            if(responseInfo.getEntries() != null) {
                result.setEntries(asyncResult.getResult().getEntries());
                break;
            }
        }
        return result;
    }
}
