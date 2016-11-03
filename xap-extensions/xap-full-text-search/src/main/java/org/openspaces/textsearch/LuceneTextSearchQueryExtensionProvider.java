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

package org.openspaces.textsearch;

import com.gigaspaces.query.extension.QueryExtensionManager;
import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.gigaspaces.query.extension.metadata.DefaultQueryExtensionPathInfo;
import com.gigaspaces.query.extension.metadata.QueryExtensionPathInfo;
import com.gigaspaces.query.extension.metadata.QueryExtensionPropertyInfo;

import org.openspaces.spatial.lucene.common.BaseLuceneQueryExtensionProvider;
import org.openspaces.spatial.lucene.common.Utils;

import java.lang.annotation.Annotation;
import java.util.Properties;

/**
 * @author Vitaliy_Zinchenko
 */
public class LuceneTextSearchQueryExtensionProvider extends BaseLuceneQueryExtensionProvider {

    public static final String NAMESPACE = "text";
    private final Properties _customProperties;

    public LuceneTextSearchQueryExtensionProvider() {
        this(new Properties());
    }

    public LuceneTextSearchQueryExtensionProvider(Properties customProperties) {
        this._customProperties = customProperties;
    }

    @Override
    public String getNamespace() {
        return NAMESPACE;
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
            SpaceTextIndex index = (SpaceTextIndex) annotation;
            addIndex(result, Utils.makePath(property, index.path()));
        } else if (annotation instanceof SpaceTextIndexes) {
            SpaceTextIndexes indexes = (SpaceTextIndexes)annotation;
            for (SpaceTextIndex index: indexes.value()) {
                addIndex(result, Utils.makePath(property, index.path()));
            }
        } else if (annotation instanceof SpaceTextAnalyzer) {
            SpaceTextAnalyzer analyzer = (SpaceTextAnalyzer) annotation;
            String path = Utils.makePath(property, analyzer.path());
            QueryExtensionPathInfo pathInfo = new DefaultQueryExtensionPathInfo();
            pathInfo.add(analyzer.getClass(), new TextAnalyzerQueryExtensionPathActionInfo(analyzer.clazz()));
            result.addPathInfo(path, pathInfo);
        } else if(annotation instanceof SpaceTextAnalyzers) {
            SpaceTextAnalyzers analyzers = (SpaceTextAnalyzers) annotation;
            for(SpaceTextAnalyzer analyzer: analyzers.value()) {
                addIndex(result, Utils.makePath(property, analyzer.path()));
            }
        }
        return result;
    }

    public String getCustomProperty(String key, String defaultValue) {
        return _customProperties.getProperty(key, defaultValue);
    }

    public LuceneTextSearchQueryExtensionProvider setCustomProperty(String key, String value) {
        this._customProperties.setProperty(key, value);
        return this;
    }
}
