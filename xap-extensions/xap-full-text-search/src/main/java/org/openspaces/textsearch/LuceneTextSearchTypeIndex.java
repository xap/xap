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

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.metadata.QueryExtensionAnnotationInfo;
import com.gigaspaces.query.extension.metadata.QueryExtensionPathInfo;
import com.gigaspaces.query.extension.metadata.TypeQueryExtension;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.openspaces.lucene.common.BaseLuceneConfiguration;
import org.openspaces.lucene.common.BaseLuceneTypeIndex;
import org.openspaces.lucene.common.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Vitaliy_Zinchenko
 * @since 12.1
 */
public class LuceneTextSearchTypeIndex extends BaseLuceneTypeIndex {

    private Map<String, Analyzer> _fieldAnalyzers;

    public LuceneTextSearchTypeIndex(BaseLuceneConfiguration luceneConfig, String namespace, SpaceTypeDescriptor typeDescriptor) throws IOException {
        super(luceneConfig, namespace, typeDescriptor);
    }

    public Analyzer getAnalyzerForPath(String path) {
        Analyzer fieldAnalyzer = _fieldAnalyzers.get(path);
        if (fieldAnalyzer != null) {
            return fieldAnalyzer;
        }
        return luceneConfig.getDefaultAnalyzer();
    }

    @Override
    protected Analyzer createAnalyzer(BaseLuceneConfiguration luceneConfig, SpaceTypeDescriptor typeDescriptor) {
        _fieldAnalyzers = createFieldAnalyzers(typeDescriptor);
        return new PerFieldAnalyzerWrapper(luceneConfig.getDefaultAnalyzer(), _fieldAnalyzers);
    }

    private Map<String, Analyzer> createFieldAnalyzers(SpaceTypeDescriptor typeDescriptor) {
        Map<String, Analyzer> analyzerMap = new HashMap<String, Analyzer>();
        TypeQueryExtension type = typeDescriptor.getQueryExtensions().getByNamespace(LuceneTextSearchQueryExtensionProvider.NAMESPACE);
        for (String path : type.getPaths()) {
            QueryExtensionPathInfo pathInfo = type.get(path);
            for (QueryExtensionAnnotationInfo annotationInfo: pathInfo.getAnnotations()) {
                if (SpaceTextAnalyzer.class.equals(annotationInfo.getType()) && annotationInfo instanceof TextAnalyzerQueryExtensionAnnotationInfo) {
                    TextAnalyzerQueryExtensionAnnotationInfo analyzerAnnotation = (TextAnalyzerQueryExtensionAnnotationInfo) annotationInfo;
                    addAnalyzer(analyzerMap, path, analyzerAnnotation.getAnalazerClass());
                }
            }
        }
        return analyzerMap;
    }

    private void addAnalyzer(Map<String, Analyzer> analyzerMap, String path, Class clazz) {
        Analyzer analyzer = Utils.createAnalyzer(clazz);
        analyzerMap.put(path, analyzer);
    }
}
