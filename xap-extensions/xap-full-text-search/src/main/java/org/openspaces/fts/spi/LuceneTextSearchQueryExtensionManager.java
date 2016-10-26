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

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.openspaces.spatial.lucene.common.spi.BaseLuceneConfiguration;
import org.openspaces.spatial.lucene.common.spi.BaseLuceneQueryExtensionManager;
import org.openspaces.spatial.lucene.common.spi.BaseLuceneTypeIndex;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * @author yechielf
 * @since 11.0
 */
public class LuceneTextSearchQueryExtensionManager extends BaseLuceneQueryExtensionManager {
    private static final Logger _logger = Logger.getLogger(LuceneTextSearchQueryExtensionManager.class.getName());
    public static final String SEARCH_OPERATION_NAME = "search";

    private QueryExtensionRuntimeInfo _info;

    protected LuceneTextSearchQueryExtensionManager(LuceneTextSearchQueryExtensionProvider provider, QueryExtensionRuntimeInfo info, LuceneTextSearchConfiguration configuration) {
        super(provider, info, configuration);
        this._info = info;
    }

    @Override
    public boolean accept(String operation, Object leftOperand, Object rightOperand) {
        return true; //TODO implement
    }

    @Override
    protected Field[] convertField(String path, Object fieldValue) {
        if (!(fieldValue instanceof String)) {
            throw new IllegalArgumentException("Field '" + path + "' with value '" + fieldValue + "' is not String.");
        }
        Field field = new TextField(path, (String) fieldValue, Field.Store.NO);
        return new Field[]{field};
    }

    @Override
    protected BaseLuceneTypeIndex createTypeIndex(BaseLuceneConfiguration luceneConfig, String namespace, SpaceTypeDescriptor typeDescriptor) throws IOException {
        return new LuceneTextSearchTypeIndex(luceneConfig, namespace, typeDescriptor);
    }

    @Override
    protected Query createQuery(String path, String operationName, Object operand) {
        validateOperationName(operationName);
        try {
            return new QueryParser("content", new StandardAnalyzer()).parse("content:" + operand); //TODO StandardAnalyzer
        } catch (ParseException e) {
            throw new IllegalArgumentException("Couldn't create full text search query for path=" + path + " operationName=" + operationName + " operand=" + operand);
        }
    }

    private void validateOperationName(String operationName) {
        if (!SEARCH_OPERATION_NAME.equals(operationName)) {
            throw new IllegalArgumentException("Provided operationName=" + operationName + " is illegal. Correct one is '" + SEARCH_OPERATION_NAME + "'");
        }
    }

}
