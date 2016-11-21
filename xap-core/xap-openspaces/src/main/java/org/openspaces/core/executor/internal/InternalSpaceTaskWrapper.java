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
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.kernel.ClassLoaderHelper;
import net.jini.core.transaction.Transaction;
import org.jini.rio.boot.ServiceClassLoader;
import org.jini.rio.boot.SupportCodeChangeAnnotationContainer;
import org.openspaces.core.executor.SupportCodeChange;
import org.openspaces.core.executor.Task;
import org.openspaces.core.transaction.manager.ExistingJiniTransactionManager;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An internal implemenation of {@link SpaceTask} that wraps the actual {@link
 * org.openspaces.core.executor.Task} to be executed.
 *
 * @author kimchy
 */
public class InternalSpaceTaskWrapper<T extends Serializable> implements SpaceTask<T>, SpaceTaskWrapper, Externalizable {
    final private static Logger logger = Logger.getLogger("com.gigaspaces.lrmi.classloading.level");

    private static final long serialVersionUID = -7391977361461247102L;

    private Task<T> task;

    private Object routing;

    private SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer;

    public InternalSpaceTaskWrapper() {
    }

    public InternalSpaceTaskWrapper(Task<T> task, Object routing) {
        this();
        this.task = task;
        this.routing = routing;
        if (this instanceof InternalDistributedSpaceTaskWrapper && task.getClass().isAnnotationPresent(SupportCodeChange.class)) {
            SupportCodeChange supportCodeChange = task.getClass().getAnnotation(SupportCodeChange.class);
            this.supportCodeChangeAnnotationContainer = new SupportCodeChangeAnnotationContainer(supportCodeChange.version());
        }
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
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v12_1_0)) {
            IOUtils.writeObject(out, supportCodeChangeAnnotationContainer);
        }
        else if (version.greaterOrEquals(PlatformLogicalVersion.v10_2_0_PATCH2)) {
            out.writeBoolean(supportCodeChangeAnnotationContainer != null);
        }
        out.writeObject(task);
        out.writeObject(routing);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v12_1_0)) {
            supportCodeChangeAnnotationContainer  = IOUtils.readObject(in);
        }
        else if (version.greaterOrEquals(PlatformLogicalVersion.v10_2_0_PATCH2)) {
            boolean supportCodeChange = in.readBoolean();
            if(supportCodeChange){
                supportCodeChangeAnnotationContainer = SupportCodeChangeAnnotationContainer.ONE_TIME;
            }
        }
        if (supportCodeChangeAnnotationContainer != null) {
            task = readTaskUsingFreshClassLoader(in, supportCodeChangeAnnotationContainer);
        }
        else {
            //noinspection unchecked
            task = (Task<T>) in.readObject();
        }
        routing = in.readObject();
    }

    /**
     * Tasks are loaded with a fresh class loader. When the task is done this fresh class loader is
     * removed. This will make it possible to load a modified version of this class GS-12351-
     * Running Distributed Task can throw ClassNotFoundException, if this task was loaded by a
     * client that already shutdown aInternalSpaceTaskWrapper.java:155nd the class has more dependencies to load. GS-12352 -
     * Distributed Task class is not unloaded after the task finish. GS-12295 - Distributed task -
     * improve class loading mechanism.
     *
     * @see com.gigaspaces.internal.server.space.SpaceImpl#executeTask(SpaceTask, Transaction,
     * SpaceContext, boolean)
     */
    private Task<T> readTaskUsingFreshClassLoader(ObjectInput in, SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer) throws ClassNotFoundException, IOException {
        ClassLoader current = ClassLoaderHelper.getContextClassLoader();
        try {
            if(current instanceof ServiceClassLoader){
                ClassLoader taskClassLoader = ((ServiceClassLoader) current).getTaskClassLoader(supportCodeChangeAnnotationContainer);
                if(logger.isLoggable(Level.FINEST)){
                    logger.finest("contextClassLoader ["+current+"] is instanceof ServiceClassLoader, asked for cached class-loader." +
                            "["+taskClassLoader +"] set to be ContextClassLoader of Thread ["+Thread.currentThread()+"]");
                }
                ClassLoaderHelper.setContextClassLoader(taskClassLoader, true);
            }
            else {
                throw new UnsupportedOperationException("supportCodeChange annotation is supported if the current ContextClassLoader is ["+ServiceClassLoader.class+"], but ContextClassLoader is ["+current+"]");
            }
            //noinspection unchecked
            return (Task<T>) in.readObject();
        } finally {
            ClassLoaderHelper.setContextClassLoader(current, true);

        }
    }

    @Override
    public boolean isOneTime() {
        return supportCodeChangeAnnotationContainer != null && supportCodeChangeAnnotationContainer.getVersion().isEmpty();
    }

}
