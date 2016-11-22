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

package org.openspaces.spatial.spi;

import com.gigaspaces.query.extension.QueryExtensionManager;
import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.gigaspaces.query.extension.metadata.QueryExtensionPathInfo;
import com.gigaspaces.query.extension.metadata.impl.DefaultQueryExtensionAnnotationInfo;
import com.gigaspaces.query.extension.metadata.impl.QueryExtensionPathInfoImpl;

import org.openspaces.spatial.SpaceSpatialIndex;
import org.openspaces.spatial.SpaceSpatialIndexes;
import org.openspaces.lucene.common.BaseLuceneQueryExtensionProvider;
import org.openspaces.lucene.common.Utils;

import java.lang.annotation.Annotation;
import java.util.Properties;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
public class LuceneSpatialQueryExtensionProvider extends BaseLuceneQueryExtensionProvider {

    public LuceneSpatialQueryExtensionProvider() {
        this(new Properties());
    }

    public LuceneSpatialQueryExtensionProvider(Properties customProperties) {
        super(customProperties);
    }

    @Override
    public String getNamespace() {
        return "spatial";
    }

    @Override
    public QueryExtensionManager createManager(QueryExtensionRuntimeInfo info) {
        LuceneSpatialConfiguration configuration = new LuceneSpatialConfiguration(this, info);
        return new LuceneSpatialQueryExtensionManager(this, info, configuration);
    }

    @Override
    public QueryExtensionPropertyInfo getPropertyExtensionInfo(String property, Annotation annotation) {
        QueryExtensionPropertyInfo result = new QueryExtensionPropertyInfo();
        if (annotation instanceof SpaceSpatialIndex) {
            SpaceSpatialIndex index = (SpaceSpatialIndex) annotation;
            String path = Utils.makePath(property, index.path());
            result.addPathInfo(path, createPathInfo(index));
        } else if (annotation instanceof SpaceSpatialIndexes) {
            SpaceSpatialIndex[] indexes = ((SpaceSpatialIndexes) annotation).value();
            for (SpaceSpatialIndex index : indexes) {
                String path = Utils.makePath(property, index.path());
                result.addPathInfo(path, createPathInfo(index));
            }
        }
        return result;
    }

    protected QueryExtensionPathInfo createPathInfo(SpaceSpatialIndex index) {
        return new QueryExtensionPathInfoImpl(new DefaultQueryExtensionAnnotationInfo(index.annotationType()));
    }

    public LuceneSpatialQueryExtensionProvider setCustomProperty(String key, String value) {
        this._customProperties.setProperty(key, value);
        return this;
    }
}
