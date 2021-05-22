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


package org.openspaces.core.executor.juc;

import com.gigaspaces.serialization.SmartExternalizable;
import org.openspaces.core.executor.Task;
import org.openspaces.core.executor.TaskRoutingProvider;
import org.openspaces.core.executor.support.ProcessObjectsProvider;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * An adapter allowing to execute a {@link Runnable} using Space task executors.
 *
 * @author kimchy
 */
public class RunnableTaskAdapter<T extends Serializable> implements Task<T>, ProcessObjectsProvider, TaskRoutingProvider, SmartExternalizable {

    private static final long serialVersionUID = -5958775825432335114L;

    private Runnable runnable;

    private T result;

    /**
     * Here just for externlizable.
     */
    public RunnableTaskAdapter() {
    }

    /**
     * Constructs a new runnable task adapter with the runnable to <code>run</code>.
     */
    public RunnableTaskAdapter(Runnable runnable) {
        this.runnable = runnable;
    }

    /**
     * Constructs a new runnable task adapter with the runnable to <code>run</code>. Will return the
     * result after the call to <code>run</code>.
     */
    public RunnableTaskAdapter(Runnable runnable, T result) {
        this.runnable = runnable;
        this.result = result;
    }

    /**
     * Simply delegates the execution to {@link Runnable#run()} and returns the optional result
     * provided in the constructor.
     */
    public T execute() throws Exception {
        runnable.run();
        return result;
    }

    /**
     * Returns the callable passed so it will be processed on the node it is executed on as well.
     */
    public Object[] getObjectsToProcess() {
        return new Object[]{runnable};
    }

    /**
     * Tries to extract the routing information form the task.
     */
    public Object getRouting() {
        return runnable;
    }

    /**
     * Returns the embedded runnable.
     */
    protected Runnable getRunnable() {
        return this.runnable;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(runnable);
        if (result == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeObject(result);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        runnable = (Runnable) in.readObject();
        if (in.readBoolean()) {
            result = (T) in.readObject();
        }
    }
}