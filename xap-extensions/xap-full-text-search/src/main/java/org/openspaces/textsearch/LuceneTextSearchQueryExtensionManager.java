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
import com.gigaspaces.internal.io.FileUtils;
import com.gigaspaces.internal.utils.Assert;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.QueryExtensionEntryIterator;
import com.gigaspaces.query.extension.QueryExtensionManager;
import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.gigaspaces.query.extension.metadata.TypeQueryExtensions;
import com.gigaspaces.server.SpaceServerEntry;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * @author Vitaliy_Zinchenko
 * @since 12.1
 */
public class LuceneTextSearchQueryExtensionManager extends QueryExtensionManager {
    private static final Logger _logger = Logger.getLogger(LuceneTextSearchQueryExtensionManager.class.getName());

    public static final String SEARCH_OPERATION_NAME = "match";

    protected static final String XAP_ID = "XAP_ID";
    protected static final String XAP_ID_VERSION = "XAP_ID_VERSION";

    protected final String _namespace;
    protected final Map<String, LuceneTextSearchTypeIndex> _luceneHolderMap = new ConcurrentHashMap<String, LuceneTextSearchTypeIndex>();
    protected final LuceneTextSearchConfiguration _luceneConfiguration;

    protected LuceneTextSearchQueryExtensionManager(LuceneTextSearchQueryExtensionProvider provider, QueryExtensionRuntimeInfo info, LuceneTextSearchConfiguration configuration) {
        super(info);
        _namespace = provider.getNamespace();
        _luceneConfiguration = configuration;
        File location = new File(_luceneConfiguration.getLocation());
        FileUtils.deleteFileOrDirectoryIfExists(location);
    }

    @Override
    public void close() throws IOException {
        for (LuceneTextSearchTypeIndex luceneHolder : _luceneHolderMap.values())
            luceneHolder.close();

        _luceneHolderMap.clear();
        FileUtils.deleteFileOrDirectoryIfExists(new File(_luceneConfiguration.getLocation()));
        super.close();
    }

    @Override
    public void registerType(SpaceTypeDescriptor typeDescriptor) {
        super.registerType(typeDescriptor);
        final String typeName = typeDescriptor.getTypeName();
        if (!_luceneHolderMap.containsKey(typeName)) {
            try {
                _luceneHolderMap.put(typeName, new LuceneTextSearchTypeIndex(_luceneConfiguration, _namespace, typeDescriptor));
            } catch (IOException e) {
                throw new SpaceRuntimeException("Failed to register type " + typeName, e);
            }
        } else {
            _logger.log(Level.WARNING, "Type [" + typeName + "] is already registered");
        }
    }

    protected LuceneTextSearchTypeIndex createTypeIndex(LuceneTextSearchConfiguration luceneConfig, String namespace, SpaceTypeDescriptor typeDescriptor) throws IOException {
        return new LuceneTextSearchTypeIndex(luceneConfig, namespace, typeDescriptor);
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


    @Override
    public boolean insertEntry(SpaceServerEntry entry, boolean hasPrevious) {
        final String typeName = entry.getSpaceTypeDescriptor().getTypeName();
        final LuceneTextSearchTypeIndex luceneHolder = _luceneHolderMap.get(typeName);
        try {
            final Document doc = createDocumentIfNeeded(luceneHolder, entry);
            // Add new
            if (doc != null)
                luceneHolder.getIndexWriter().addDocument(doc);
            // Delete old
            if (hasPrevious) {
                TermQuery query = new TermQuery(new Term(XAP_ID_VERSION, concat(entry.getUid(), entry.getVersion() - 1)));
                luceneHolder.getIndexWriter().deleteDocuments(query);
            }
            // Flush
            if (doc != null || hasPrevious)
                luceneHolder.commit(false);
            return doc != null;
        } catch (Exception e) {
            String operation = hasPrevious ? "update" : "insert";
            throw new SpaceRuntimeException("Failed to " + operation + " entry of type " + typeName + " with id [" + entry.getUid() + "]", e);
        }
    }


    protected String concat(String uid, int version) {
        return uid + "_" + version;
    }

    protected Document createDocumentIfNeeded(LuceneTextSearchTypeIndex luceneHolder, SpaceServerEntry entry) {
        TypeQueryExtensions queryExtensions = entry.getSpaceTypeDescriptor().getQueryExtensions();
        Document doc = null;
        for (String path : luceneHolder.getQueryExtensionInfo().getPaths()) {
            if(!queryExtensions.isIndexed(_namespace, path)) {
                continue;
            }
            final Object fieldValue = entry.getPathValue(path);
            if(fieldValue == null) {
                continue;
            }
            Field[] fields = convertField(path, fieldValue);
            if (doc == null && fields.length != 0) {
                doc = new Document();
            }
            for (Field field : fields) {
                doc.add(field);
            }
        }
        if (doc != null) {
            //cater for uid & version
            //noinspection deprecation
            doc.add(new Field(XAP_ID, entry.getUid(), Field.Store.YES, Field.Index.NO));
            //noinspection deprecation
            doc.add(new Field(XAP_ID_VERSION, concat(entry.getUid(), entry.getVersion()), Field.Store.YES, Field.Index.NOT_ANALYZED));
        }

        return doc;
    }

    protected Field[] convertField(String path, Object fieldValue) {
        if (!(fieldValue instanceof String)) {
            throw new IllegalArgumentException("Field '" + path + "' with value '" + fieldValue + "' is not String. " +
                    "Try to use 'path' of the @" + SpaceTextAnalyzer.class.getSimpleName() + " or @" + SpaceTextIndex.class.getSimpleName());
        }
        Field field = new TextField(path, (String) fieldValue, Field.Store.NO);
        return new Field[]{field};
    }

    private Analyzer getAnalyzer(String typeName, String path) {
        LuceneTextSearchTypeIndex typeIndex = _luceneHolderMap.get(typeName);
        return typeIndex != null ? typeIndex.getAnalyzerForPath(path) : _luceneConfiguration.getDefaultAnalyzer();
    }


    @Override
    public QueryExtensionEntryIterator queryByIndex(String typeName, String path, String operationName, Object operand) {
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "query [typeName=" + typeName + ", path=" + path + ", operation=" + operationName + ", operand=" + operand + "]");

        final Query query = createQuery(typeName, path, operationName, operand);
        final LuceneTextSearchTypeIndex luceneHolder = _luceneHolderMap.get(typeName);
        try {
            // Flush
            luceneHolder.commit(true);

            DirectoryReader dr = DirectoryReader.open(luceneHolder.getDirectory());
            IndexSearcher is = new IndexSearcher(dr);
            ScoreDoc[] scores = is.search(query, _luceneConfiguration.getMaxResults()).scoreDocs;
            return new LuceneQueryExtensionEntryIterator(scores, is, dr);
        } catch (IOException e) {
            throw new SpaceRuntimeException("Failed to scan index", e);
        }
    }

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

    @Override
    public void removeEntry(SpaceTypeDescriptor typeDescriptor, String uid, int version) {
        final String typeName = typeDescriptor.getTypeName();
        final LuceneTextSearchTypeIndex luceneHolder = _luceneHolderMap.get(typeName);
        try {
            luceneHolder.getIndexWriter().deleteDocuments(new TermQuery(new Term(XAP_ID_VERSION, concat(uid, version))));
            luceneHolder.commit(false);
        } catch (IOException e) {
            throw new SpaceRuntimeException("Failed to remove entry of type " + typeName, e);
        }
    }

}
