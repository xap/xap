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

package org.openspaces.fts.spi;

import com.gigaspaces.query.extension.QueryExtensionManager;
import com.gigaspaces.query.extension.QueryExtensionProvider;
import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.gigaspaces.query.extension.metadata.DefaultQueryExtensionPathInfo;
import com.gigaspaces.query.extension.metadata.QueryExtensionPropertyInfo;

import org.openspaces.fts.SpaceTextIndex;
import org.openspaces.fts.SpaceTextIndexes;
import org.openspaces.spatial.lucene.common.spi.BaseLuceneConfiguration;
import org.openspaces.spatial.lucene.common.spi.BaseLuceneQueryExtensionProvider;

import java.lang.annotation.Annotation;
import java.util.Properties;

/**
 * @author Vitaliy_Zinchenko
 */
public class LuceneTextSearchQueryExtensionProvider extends BaseLuceneQueryExtensionProvider {

    private final Properties _customProperties;

    public LuceneTextSearchQueryExtensionProvider() {
        this(new Properties());
    }

    public LuceneTextSearchQueryExtensionProvider(Properties customProperties) {
        this._customProperties = customProperties;
    }

    @Override
    public String getNamespace() {
        return "text";
    }

    @Override
    public QueryExtensionManager createManager(QueryExtensionRuntimeInfo info) {
        LuceneTextSearchConfiguration configuration = new LuceneTextSearchConfiguration(this, info);
        return new LuceneTextSearchQueryExtensionManager(this, info, configuration);
    }

    @Override
    public QueryExtensionPropertyInfo getPropertyExtensionInfo(String property, Annotation annotation) {
        QueryExtensionPropertyInfo result = new QueryExtensionPropertyInfo();
        if (annotation instanceof SpaceTextIndex) {
            addIndex(result, path(property, (SpaceTextIndex) annotation));
        } else if (annotation instanceof SpaceTextIndexes) {
            SpaceTextIndexes indexes = (SpaceTextIndexes)annotation;
            for (SpaceTextIndex index: indexes.value()) {
                addIndex(result, path(property, index));
            }
        }
        return result;
    }

    private void addIndex(QueryExtensionPropertyInfo result, String path) {
        result.addPathInfo(path, new DefaultQueryExtensionPathInfo());
    }

    private static String path(String property, SpaceTextIndex index) {
        return index.path().length() == 0 ? property : property + "." + index.path();
    }

    public String getCustomProperty(String key, String defaultValue) {
        return _customProperties.getProperty(key, defaultValue);
    }

    public LuceneTextSearchQueryExtensionProvider setCustomProperty(String key, String value) {
        this._customProperties.setProperty(key, value);
        return this;
    }
}
