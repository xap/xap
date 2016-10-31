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

import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.openspaces.spatial.lucene.common.BaseLuceneConfiguration;
import org.openspaces.spatial.lucene.common.BaseLuceneQueryExtensionProvider;
import org.openspaces.spatial.lucene.common.Utils;

/**
 * @author Vitaliy_Zinchenko
 */
public class LuceneTextSearchConfiguration extends BaseLuceneConfiguration {

    public static final String INDEX_LOCATION_FOLDER_NAME = "full_text_search";

    public static final String STORAGE_LOCATION = "lucene.full.text.search.storage.location";
    public static final String MAX_UNCOMMITED_CHANGES = "lucene.full.text.search.max.uncommited.changes";

    public static final String STORAGE_DIRECTORYTYPE = "lucene.full.text.search.storage.directory-type";

    public static final String DEFAULT_ANALYZER_PROPERTY_KEY = "lucene.full.text.search.default.analyzer";
    private Analyzer _defaultAnalyzer;

    public LuceneTextSearchConfiguration(BaseLuceneQueryExtensionProvider provider, QueryExtensionRuntimeInfo info) {
        super(provider, info);
        this._defaultAnalyzer = initDefaultAnalyzer(provider);
    }

    private Analyzer initDefaultAnalyzer(BaseLuceneQueryExtensionProvider provider) {
        String analyzerClassName = provider.getCustomProperty(DEFAULT_ANALYZER_PROPERTY_KEY, StandardAnalyzer.class.getName());
        try {
            return Utils.createAnalyzer(this.getClass().getClassLoader().loadClass(analyzerClassName)); //TODO refactor/ try to reuse existing utils
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Failed to load analyzer class " + analyzerClassName + ". Check property " + DEFAULT_ANALYZER_PROPERTY_KEY);
        }
    }

    @Override
    protected String getMaxUncommitedChangesPropertyKey() {
        return MAX_UNCOMMITED_CHANGES;
    }

    @Override
    protected String getIndexLocationFolderName() {
        return INDEX_LOCATION_FOLDER_NAME;
    }

    @Override
    protected String getStorageLocationPropertyKey() {
        return STORAGE_LOCATION;
    }

    @Override
    protected String getStorageDirectoryTypePropertyKey() {
        return STORAGE_DIRECTORYTYPE;
    }

    @Override
    public Analyzer getDefaultAnalyzer() {
        return _defaultAnalyzer;
    }
}
