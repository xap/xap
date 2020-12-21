/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.internal.space.requests;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.responses.BroadcastTableSpaceResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public abstract class BroadcastTableSpaceRequestInfo extends AbstractSpaceRequestInfo {
    private static final long serialVersionUID = 1L;
    public enum Action {
        PUSH_ENTRY(0),
        PUSH_ENTRIES (1);

        public final byte value;
        Action(int value) {
            this.value = (byte)  value;
        }
    }

    public abstract Action getAction();

    public BroadcastTableSpaceResponseInfo reduce(List<AsyncResult<BroadcastTableSpaceResponseInfo>> asyncResults) throws Exception {
        BroadcastTableSpaceResponseInfo result = new BroadcastTableSpaceResponseInfo();
        for (AsyncResult<BroadcastTableSpaceResponseInfo> asyncResult : asyncResults){
            if(asyncResult.getException() != null) {
                throw asyncResult.getException();
            }
            result.getExceptionMap().putAll(asyncResult.getResult().getExceptionMap());
        }
        return result;
    }
}
