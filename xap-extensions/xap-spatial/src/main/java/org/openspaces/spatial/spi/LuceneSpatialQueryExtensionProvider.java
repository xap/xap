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
import com.gigaspaces.query.extension.QueryExtensionProvider;
import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.gigaspaces.query.extension.metadata.DefaultQueryExtensionPathInfo;
import com.gigaspaces.query.extension.metadata.QueryExtensionPropertyInfo;

import org.openspaces.spatial.SpaceSpatialIndex;
import org.openspaces.spatial.SpaceSpatialIndexes;

import java.lang.annotation.Annotation;
import java.util.Properties;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
public class LuceneSpatialQueryExtensionProvider extends QueryExtensionProvider {

    private final Properties _customProperties;

    public LuceneSpatialQueryExtensionProvider() {
        this(new Properties());
    }

    public LuceneSpatialQueryExtensionProvider(Properties customProperties) {
        this._customProperties = customProperties;
    }

    @Override
    public String getNamespace() {
        return "spatial";
    }

    @Override
    public QueryExtensionManager createManager(QueryExtensionRuntimeInfo info) {
        return new LuceneSpatialQueryExtensionManager(this, info);
    }

    private static String path(String property, SpaceSpatialIndex index) {
        return index.path().length() == 0 ? property : property + "." + index.path();
    }

    @Override
    public QueryExtensionPropertyInfo getPropertyExtensionInfo(String property, Annotation annotation) {
        QueryExtensionPropertyInfo result = new QueryExtensionPropertyInfo();
        if (annotation instanceof SpaceSpatialIndex) {
            SpaceSpatialIndex index = (SpaceSpatialIndex) annotation;
            result.addPathInfo(path(property, index), new DefaultQueryExtensionPathInfo());
        } else if (annotation instanceof SpaceSpatialIndexes) {
            SpaceSpatialIndex[] indexes = ((SpaceSpatialIndexes) annotation).value();
            for (SpaceSpatialIndex index : indexes)
                result.addPathInfo(path(property, index), new DefaultQueryExtensionPathInfo());
        }
        return result;
    }

    public String getCustomProperty(String key, String defaultValue) {
        return _customProperties.getProperty(key, defaultValue);
    }

    public LuceneSpatialQueryExtensionProvider setCustomProperty(String key, String value) {
        this._customProperties.setProperty(key, value);
        return this;
    }
}
