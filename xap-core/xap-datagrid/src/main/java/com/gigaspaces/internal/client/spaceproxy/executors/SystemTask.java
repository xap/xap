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

import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.IJSpace;

import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
public abstract class SystemTask<T extends Serializable> implements SpaceTask<T>, SmartExternalizable {
    private static final long serialVersionUID = 1L;

    public abstract SpaceRequestInfo getSpaceRequestInfo();

    @Override
    public T execute(IJSpace spaceProxy, Transaction tx) throws Exception {
        SpaceImpl space = spaceProxy.getDirectProxy().getSpaceImplIfEmbedded();
        return (T) space.executeAction(this);
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
    }
}
