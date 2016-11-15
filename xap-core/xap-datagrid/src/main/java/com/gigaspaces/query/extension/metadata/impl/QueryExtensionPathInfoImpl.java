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

package com.gigaspaces.query.extension.metadata.impl;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.query.extension.metadata.QueryExtensionAnnotationAttributesInfo;
import com.gigaspaces.query.extension.metadata.QueryExtensionPathInfo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class QueryExtensionPathInfoImpl implements QueryExtensionPathInfo, Externalizable{

    private Map<Class<? extends Annotation>, QueryExtensionAnnotationAttributesInfo> pathAnnotationInfo = new HashMap<Class<? extends Annotation>, QueryExtensionAnnotationAttributesInfo>();

    /**
     * Required for Externalizable
     */
    public QueryExtensionPathInfoImpl() {

    }

    public QueryExtensionPathInfoImpl(Class<? extends Annotation> annotationType, QueryExtensionAnnotationAttributesInfo attributesInfo) {
        pathAnnotationInfo.put(annotationType, attributesInfo);
    }

    public void add(Class<? extends Annotation> action, QueryExtensionAnnotationAttributesInfo attributesInfo) {
        pathAnnotationInfo.put(action, attributesInfo);
    }

    public Collection<Class<? extends Annotation>> getAnnotations() {
        return pathAnnotationInfo.keySet();
    }

    public QueryExtensionAnnotationAttributesInfo getAnnotationInfo(Class<? extends Annotation> annotationType) {
        return pathAnnotationInfo.get(annotationType);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(pathAnnotationInfo.size());
        for (Map.Entry<Class<? extends Annotation>, QueryExtensionAnnotationAttributesInfo> entry : pathAnnotationInfo.entrySet()) {
            IOUtils.writeObject(out, entry.getKey());
            IOUtils.writeObject(out, entry.getValue());
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Class<? extends Annotation> key = IOUtils.readObject(in);
            QueryExtensionAnnotationAttributesInfo value = IOUtils.readObject(in);
            pathAnnotationInfo.put(key, value);
        }
    }
}
