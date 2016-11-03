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

package com.gigaspaces.query.extension.metadata;

import com.gigaspaces.internal.io.IOUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
public abstract class QueryExtensionPathInfo implements Externalizable {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private Map<Class<? extends Annotation>, QueryExtensionPathActionInfo> pathActionInfo = new HashMap<Class<? extends Annotation>, QueryExtensionPathActionInfo>();

    public void add(Class<? extends Annotation> action, QueryExtensionPathActionInfo actionInfo) {
        pathActionInfo.put(action, actionInfo);
    }

    public Collection<Class<? extends Annotation>> getActions() {
        return pathActionInfo.keySet();
    }

    public QueryExtensionPathActionInfo getActionInfo(Class<? extends Annotation> actionType) {
        return pathActionInfo.get(actionType);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(pathActionInfo.size());
        for (Map.Entry<Class<? extends Annotation>, QueryExtensionPathActionInfo> entry : pathActionInfo.entrySet()) {
            IOUtils.writeObject(out, entry.getKey());
            IOUtils.writeObject(out, entry.getValue());
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Class<? extends Annotation> key = IOUtils.readObject(in);
            QueryExtensionPathActionInfo value = IOUtils.readObject(in);
            pathActionInfo.put(key, value);
        }
    }
}
