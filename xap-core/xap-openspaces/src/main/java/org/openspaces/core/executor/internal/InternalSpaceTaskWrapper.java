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


package org.openspaces.core.executor.internal;

import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.executor.SpaceTaskWrapper;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.utils.CodeChangeUtilities;
import com.j_spaces.core.IJSpace;
import net.jini.core.transaction.Transaction;
import org.jini.rio.boot.SupportCodeChangeAnnotationContainer;
import org.openspaces.core.executor.Task;
import org.openspaces.core.transaction.manager.ExistingJiniTransactionManager;

import java.io.*;

/**
 * An internal implemenation of {@link SpaceTask} that wraps the actual {@link
 * org.openspaces.core.executor.Task} to be executed.
 *
 * @author kimchy
 */
public class InternalSpaceTaskWrapper<T extends Serializable> implements SpaceTask<T>, SpaceTaskWrapper, SmartExternalizable {
    private static final long serialVersionUID = -7391977361461247102L;

    private Task<T> task;

    private Object routing;

    public InternalSpaceTaskWrapper() {
    }

    public InternalSpaceTaskWrapper(Task<T> task, Object routing) {
        this();
        this.task = task;
        this.routing = routing;
    }

    @Override
    public SupportCodeChangeAnnotationContainer getSupportCodeChangeAnnotationContainer() {
        return CodeChangeUtilities.createContainerFromSupportCodeAnnotationIfNeeded(task);
    }

    public T execute(IJSpace space, Transaction tx) throws Exception {
        if (tx != null) {
            try {
                ExistingJiniTransactionManager.bindExistingTransaction(tx);
                return task.execute();
            } finally {
                ExistingJiniTransactionManager.unbindExistingTransaction();
            }
        }
        return task.execute();
    }

    public Object getWrappedTask() {
        return getTask();
    }

    public Task<T> getTask() {
        return task;
    }

    @SpaceRouting
    public Object getRouting() {
        return routing;
    }

    public void setRouting(Object routing) {
        this.routing = routing;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        //PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        // Old code - can never happen starting 12.1
        //if (version.greaterOrEquals(PlatformLogicalVersion.v10_2_0_PATCH2) && version.lessThan(PlatformLogicalVersion.v12_1_0)) {
        //    out.writeBoolean(supportCodeChangeAnnotationContainer != null);
        //}
        out.writeObject(task);
        out.writeObject(routing);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer = null;
        if(version.patchSameOrGreater(PlatformLogicalVersion.v10_2_0_PATCH2) /*equal to v10_2_0_PATCH2*/
                ||
        (version.greaterOrEquals(PlatformLogicalVersion.v11_0_0) && (version.lessThan(PlatformLogicalVersion.v12_1_0)))) { /*11.0.0 <= version < 12.1*/
            boolean supportCodeChange = in.readBoolean();
            if(supportCodeChange){
                supportCodeChangeAnnotationContainer = SupportCodeChangeAnnotationContainer.ONE_TIME;
            }
        }
        task = (Task<T>) IOUtils.readObject(in, supportCodeChangeAnnotationContainer, false);
        routing = in.readObject();
    }
}
