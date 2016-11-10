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

import com.gigaspaces.SpaceRuntimeException;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.openspaces.spatial.lucene.common.BaseLuceneQueryExtensionManager;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * @author Vitaliy_Zinchenko
 */
public class LuceneTextSearchQueryExtensionManager extends BaseLuceneQueryExtensionManager<LuceneTextSearchConfiguration, LuceneTextSearchTypeIndex> {
    private static final Logger _logger = Logger.getLogger(LuceneTextSearchQueryExtensionManager.class.getName());
    public static final String SEARCH_OPERATION_NAME = "search";

    protected LuceneTextSearchQueryExtensionManager(LuceneTextSearchQueryExtensionProvider provider, QueryExtensionRuntimeInfo info, LuceneTextSearchConfiguration configuration) {
        super(provider, info, configuration);
    }

    @Override
    public boolean accept(String typeName, String path, String operation, Object gridValue, Object luceneQuery) {
        Assert.notNull(gridValue, "Provided value from grid is null");
        Assert.notNull(luceneQuery, "Provided lucene query is null");
        validateOperationName(operation);

        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "filter [operation=" + operation + ", leftOperand(value from grid)=" + gridValue + ", rightOperand(lucene query)=" + luceneQuery + "]");

        try {
            Analyzer analyzer = getAnalyzer(typeName, path);
            MemoryIndex index = new MemoryIndex();
            index.addField("content", String.valueOf(gridValue), analyzer);
            Query query = new QueryParser("content", analyzer).parse(String.valueOf(luceneQuery));
            float score = index.search(query);
            return score > 0.0f;
        } catch (ParseException e) {
            throw new SpaceRuntimeException("Could not parse full text query [ " + luceneQuery + " ]", e);
        }
    }

    private Analyzer getAnalyzer(String typeName, String path) {
        LuceneTextSearchTypeIndex typeIndex = _luceneHolderMap.get(typeName);
        return typeIndex != null ? typeIndex.getAnalyzerForPath(path) : _luceneConfiguration.getDefaultAnalyzer();
    }

    @Override
    protected Field[] convertField(String path, Object fieldValue) {
        if (!(fieldValue instanceof String)) {
            throw new IllegalArgumentException("Field '" + path + "' with value '" + fieldValue + "' is not String. " +
                    "Try to use 'path' of the @" + SpaceTextAnalyzer.class.getSimpleName() + " or @" + SpaceTextIndex.class.getSimpleName());
        }
        Field field = new TextField(path, (String) fieldValue, Field.Store.NO);
        return new Field[]{field};
    }

    @Override
    protected LuceneTextSearchTypeIndex createTypeIndex(LuceneTextSearchConfiguration luceneConfig, String namespace, SpaceTypeDescriptor typeDescriptor) throws IOException {
        return new LuceneTextSearchTypeIndex(luceneConfig, namespace, typeDescriptor);
    }

    @Override
    protected Query createQuery(String typeName, String path, String operationName, Object operand) {
        Assert.notNull(operand, "Provided operand is null");
        validateOperationName(operationName);
        try {
            LuceneTextSearchTypeIndex typeIndex = _luceneHolderMap.get(typeName);
            Analyzer analyzer = typeIndex.getAnalyzerForPath(path);
            return new QueryParser(path, analyzer).parse(path + ":" + operand);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Couldn't create full text search query for path=" + path + " operationName=" + operationName + " operand=" + operand, e);
        }
    }

    private void validateOperationName(String operationName) {
        if (!SEARCH_OPERATION_NAME.equals(operationName)) {
            throw new IllegalArgumentException("Provided operationName=" + operationName + " is incorrect. Correct one is '" + SEARCH_OPERATION_NAME + "'");
        }
    }

}
