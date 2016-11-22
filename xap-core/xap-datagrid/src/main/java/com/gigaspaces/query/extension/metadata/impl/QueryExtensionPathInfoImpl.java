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
import com.gigaspaces.query.extension.metadata.QueryExtensionAnnotationInfo;
import com.gigaspaces.query.extension.metadata.QueryExtensionPathInfo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class QueryExtensionPathInfoImpl implements QueryExtensionPathInfo, Externalizable {

    private static final long serialVersionUID = 1L;

    private List<QueryExtensionAnnotationInfo> pathAnnotationInfos = new ArrayList<QueryExtensionAnnotationInfo>();

    /**
     * Required for Externalizable
     */
    public QueryExtensionPathInfoImpl() {

    }

    public QueryExtensionPathInfoImpl(Class<? extends Annotation> annotationType) {
        pathAnnotationInfos.add(new DefaultQueryExtensionAnnotationInfo(annotationType));
    }

    public QueryExtensionPathInfoImpl(QueryExtensionAnnotationInfo annotationInfo) {
        pathAnnotationInfos.add(annotationInfo);
    }

    public void add(QueryExtensionAnnotationInfo annotationInfo) {
        pathAnnotationInfos.add(annotationInfo);
    }

    @Override
    public List<QueryExtensionAnnotationInfo> getAnnotations() {
        return pathAnnotationInfos;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(pathAnnotationInfos.size());
        for (QueryExtensionAnnotationInfo annotationInfo : pathAnnotationInfos) {
            IOUtils.writeObject(out, annotationInfo);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            QueryExtensionAnnotationInfo annotationInfo = IOUtils.readObject(in);
            pathAnnotationInfos.add(annotationInfo);
        }
    }
}
