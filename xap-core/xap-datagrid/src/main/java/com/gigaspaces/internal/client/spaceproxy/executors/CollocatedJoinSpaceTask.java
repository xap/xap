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
package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.space.requests.CollocatedJoinSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.CollocatedJoinSpaceResponseInfo;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.query.JoinedQueryResult;
import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author yohanakh
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class CollocatedJoinSpaceTask extends SystemDistributedTask<CollocatedJoinSpaceResponseInfo> {
    private static final long serialVersionUID = 1L;

    private CollocatedJoinSpaceRequestInfo _collocatedJoinSpaceRequestInfo;

    public CollocatedJoinSpaceTask() {
    }

    public CollocatedJoinSpaceTask(AbstractDMLQuery query, Transaction txn, int readModifier, int max) {
        this._collocatedJoinSpaceRequestInfo = new CollocatedJoinSpaceRequestInfo(query, txn, readModifier, max);
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return _collocatedJoinSpaceRequestInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(_collocatedJoinSpaceRequestInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this._collocatedJoinSpaceRequestInfo = (CollocatedJoinSpaceRequestInfo) in.readObject();
    }

    @Override
    public CollocatedJoinSpaceResponseInfo reduce(List<AsyncResult<CollocatedJoinSpaceResponseInfo>> asyncResults) throws Exception {
        JoinedQueryResult res = new JoinedQueryResult();
        for (AsyncResult<CollocatedJoinSpaceResponseInfo> asyncResult : asyncResults) {
            if (asyncResult.getException() != null) {
                throw new RuntimeException(asyncResult.getException());
            }
            res.addAll(asyncResult.getResult().getResult());
        }

        return new CollocatedJoinSpaceResponseInfo(res);
    }
}
