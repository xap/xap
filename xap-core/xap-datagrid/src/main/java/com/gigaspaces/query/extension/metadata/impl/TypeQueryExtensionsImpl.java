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
import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.query.extension.QueryExtensionProvider;
import com.gigaspaces.query.extension.SpaceQueryExtension;
import com.gigaspaces.query.extension.impl.QueryExtensionProviderCache;
import com.gigaspaces.query.extension.metadata.typebuilder.QueryExtensionInfo;
import com.gigaspaces.query.extension.metadata.QueryExtensionAnnotationAttributesInfo;
import com.gigaspaces.query.extension.metadata.QueryExtensionPathInfo;
import com.gigaspaces.query.extension.metadata.provided.QueryExtensionPropertyInfo;
import com.gigaspaces.query.extension.metadata.provided.QueryExtensionTypeInfo;
import com.gigaspaces.query.extension.metadata.TypeQueryExtension;
import com.gigaspaces.query.extension.metadata.TypeQueryExtensions;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class TypeQueryExtensionsImpl implements TypeQueryExtensions, Externalizable {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private final Map<String, TypeQueryExtensionImpl> info = new HashMap<String, TypeQueryExtensionImpl>();

    /**
     * required for Externalizable
     */
    public TypeQueryExtensionsImpl() {
    }

    public TypeQueryExtensionsImpl(SpaceTypeInfo typeInfo) {
        for (SpacePropertyInfo property : typeInfo.getSpaceProperties()) {
            for (Annotation annotation : property.getGetterMethod().getDeclaredAnnotations())
                if (annotation.annotationType().isAnnotationPresent(SpaceQueryExtension.class)) {
                    final QueryExtensionProvider provider = extractQueryExtensionProvider(annotation);
                    final QueryExtensionPropertyInfo propertyInfo = provider.getPropertyExtensionInfo(property.getName(), annotation);
                    for (String path : propertyInfo.getPaths())
                        add(provider.getNamespace(), path, propertyInfo.getPathInfo(path));
                }
        }
    }

    private void add(String namespace, String path, QueryExtensionPathInfo pathInfo) {
        for(Class<? extends Annotation> action: pathInfo.getActions()) {
            QueryExtensionAnnotationAttributesInfo actionInfo = pathInfo.getActionInfo(action);
            add(namespace, path, action, actionInfo);
        }
    }

    private QueryExtensionProvider extractQueryExtensionProvider(Annotation annotation) {
        final SpaceQueryExtension spaceQueryExtension = annotation.annotationType().getAnnotation(SpaceQueryExtension.class);
        return QueryExtensionProviderCache.getByClass(spaceQueryExtension.providerClass());
    }

    public void add(String path, Class<? extends Annotation> annotationType) {
        add(path, annotationType, new DefaultQueryExtensionPathAnnotationAttributesInfo());
    }

    public void add(String path, QueryExtensionInfo queryExtensionInfo) {
        add(path, queryExtensionInfo.getQueryExtensionAnnotation(), queryExtensionInfo.getQueryExtensionActionInfo());
    }

    private void add(String path, Class<? extends Annotation> annotationType, QueryExtensionAnnotationAttributesInfo queryExtensionPathActionInfo) {
        add(getNamespace(annotationType), path, annotationType, queryExtensionPathActionInfo);
    }

    private String getNamespace(Class<? extends Annotation> annotationType) {
        if (!annotationType.isAnnotationPresent(SpaceQueryExtension.class))
            throw new IllegalArgumentException("Annotation " + annotationType + " is not a space query extension annotation");
        final SpaceQueryExtension spaceQueryExtension = annotationType.getAnnotation(SpaceQueryExtension.class);
        return QueryExtensionProviderCache.getByClass(spaceQueryExtension.providerClass()).getNamespace();
    }

    @Override
    public boolean isIndexed(String namespace, String path) {
        final TypeQueryExtension typeQueryExtension = info.get(namespace);
        if(typeQueryExtension == null) {
            return false;
        }
        QueryExtensionPathInfo pathInfo = typeQueryExtension.get(path);
        if(pathInfo == null) {
            return false;
        }
        for(Class<? extends Annotation> actionType: pathInfo.getActions()) {
            if(pathInfo.getActionInfo(actionType).isIndexed()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Collection<String> getNamespaces() {
        return info.keySet();
    }

    @Override
    public TypeQueryExtensionImpl getByNamespace(String namespace) {
        return info.get(namespace);
    }


    private void add(String namespace, String path, Class<? extends Annotation> annotationType, QueryExtensionAnnotationAttributesInfo actionInfo) {
        TypeQueryExtensionImpl typeQueryExtension = getOrCreateTypeQueryExtension(namespace);
        QueryExtensionPathInfo pathInfo = getOrCreatePath(typeQueryExtension, path);
        pathInfo.add(annotationType, actionInfo);
    }

    private TypeQueryExtensionImpl getOrCreateTypeQueryExtension(String namespace) {
        TypeQueryExtensionImpl typeQueryExtension = info.get(namespace);
        if (typeQueryExtension == null) {
            typeQueryExtension = new TypeQueryExtensionImpl();
            info.put(namespace, typeQueryExtension);
        }
        return typeQueryExtension;
    }

    private QueryExtensionPathInfo getOrCreatePath(TypeQueryExtensionImpl typeQueryExtension, String path) {
        QueryExtensionPathInfo pathInfo = typeQueryExtension.get(path);
        if (pathInfo == null) {
            pathInfo = new DefaultQueryExtensionPathInfo();
            typeQueryExtension.addPath(path, pathInfo);
        }
        return pathInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(info.size());
        for (Map.Entry<String, TypeQueryExtensionImpl> entry : info.entrySet()) {
            IOUtils.writeString(out, entry.getKey());
            entry.getValue().writeExternal(out);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = IOUtils.readString(in);
            TypeQueryExtensionImpl value = new TypeQueryExtensionImpl();
            value.readExternal(in);
            info.put(key, value);
        }
    }
}
