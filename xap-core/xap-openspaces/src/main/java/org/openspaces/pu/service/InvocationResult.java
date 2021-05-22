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

package org.openspaces.pu.service;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class InvocationResult implements SmartExternalizable {

    private static final long serialVersionUID = 1051610996828142259L;

    private int instanceId;
    private boolean executeOnce;
    private boolean exceptionThrownWhileExecutingOnce;
    private boolean exceptionThrownWhileExecutingOnAll;
    private Object executeOnceResult;
    private Object executeOnAllResult;

    public int getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(int instanceId) {
        this.instanceId = instanceId;
    }

    public boolean isExecuteOnce() {
        return executeOnce;
    }

    public void setExecuteOnce(boolean executeOnce) {
        this.executeOnce = executeOnce;
    }

    public Object getExecuteOnceResult() {
        return executeOnceResult;
    }

    public void setExecuteOnceResult(Object executeOnceResult) {
        this.executeOnceResult = executeOnceResult;
    }

    public Object getExecuteOnAllResult() {
        return executeOnAllResult;
    }

    public void setExecuteOnAllResult(Object executeOnAllResult) {
        this.executeOnAllResult = executeOnAllResult;
    }

    public boolean isExceptionThrownWhileExecutingOnce() {
        return exceptionThrownWhileExecutingOnce;
    }

    public void setExceptionThrownWhileExecutingOnce(boolean exceptionThrownWhileExecutingOnce) {
        this.exceptionThrownWhileExecutingOnce = exceptionThrownWhileExecutingOnce;
    }

    public boolean isExceptionThrownWhileExecutingOnAll() {
        return exceptionThrownWhileExecutingOnAll;
    }

    public void setExceptionThrownWhileExecutingOnAll(boolean exceptionThrownWhileExecutingOnAll) {
        this.exceptionThrownWhileExecutingOnAll = exceptionThrownWhileExecutingOnAll;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(instanceId);
        out.writeBoolean(executeOnce);
        out.writeBoolean(exceptionThrownWhileExecutingOnce);
        out.writeBoolean(exceptionThrownWhileExecutingOnAll);
        out.writeObject(executeOnceResult);
        out.writeObject(executeOnAllResult);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        instanceId = in.readInt();
        executeOnce = in.readBoolean();
        exceptionThrownWhileExecutingOnce = in.readBoolean();
        exceptionThrownWhileExecutingOnAll = in.readBoolean();
        executeOnceResult = in.readObject();
        executeOnAllResult = in.readObject();
    }

    @Override
    public String toString() {
        return "InvocationResult [instanceId=" + instanceId + ", executeOnce=" + executeOnce
                + ", exceptionThrownWhileExecutingOnce=" + exceptionThrownWhileExecutingOnce
                + ", exceptionThrownWhileExecutingOnAll=" + exceptionThrownWhileExecutingOnAll + ", executeOnceResult="
                + executeOnceResult + ", executeOnAllResult=" + executeOnAllResult + "]";
    }

}
