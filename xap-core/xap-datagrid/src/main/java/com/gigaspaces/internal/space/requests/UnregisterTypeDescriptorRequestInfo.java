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
import com.gigaspaces.internal.space.responses.UnregisterTypeDescriptorResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
public class UnregisterTypeDescriptorRequestInfo extends AbstractSpaceRequestInfo {
    private static final long serialVersionUID = 1L;

    public String typeName;

    /**
     * Required for Externalizable
     */
    public UnregisterTypeDescriptorRequestInfo() {
    }

    public UnregisterTypeDescriptorRequestInfo(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        IOUtils.writeString(out, typeName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this.typeName = IOUtils.readString(in);
    }

    public UnregisterTypeDescriptorResponseInfo reduce(List<AsyncResult<UnregisterTypeDescriptorResponseInfo>> results)
            throws Exception {
        UnregisterTypeDescriptorResponseInfo finalResult = null;

        for (AsyncResult<UnregisterTypeDescriptorResponseInfo> result : results) {
            if (result.getException() != null)
                throw result.getException();
            UnregisterTypeDescriptorResponseInfo responseInfo = result.getResult();
            if (responseInfo.exception != null)
                throw responseInfo.exception;
            if (finalResult == null) {
                finalResult = responseInfo;
            }
        }

        return finalResult;
    }
}
