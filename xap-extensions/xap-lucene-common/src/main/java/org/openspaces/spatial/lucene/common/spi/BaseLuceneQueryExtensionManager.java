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

package org.openspaces.spatial.lucene.common.spi;

import com.gigaspaces.SpaceRuntimeException;
import com.gigaspaces.internal.io.FileUtils;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.QueryExtensionEntryIterator;
import com.gigaspaces.query.extension.QueryExtensionManager;
import com.gigaspaces.query.extension.QueryExtensionProvider;
import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.gigaspaces.server.SpaceServerEntry;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
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
 * @author yechielf
 * @since 11.0
 */
public abstract class BaseLuceneQueryExtensionManager extends QueryExtensionManager {

    private static final Logger _logger = Logger.getLogger(BaseLuceneQueryExtensionManager.class.getName());

    protected static final String XAP_ID = "XAP_ID";
    protected static final String XAP_ID_VERSION = "XAP_ID_VERSION";
    //TODO make it configurable
    private static final int MAX_RESULTS = Integer.MAX_VALUE;

    private final String _namespace;
    protected final Map<String, BaseLuceneTypeIndex> _luceneHolderMap = new ConcurrentHashMap<String, BaseLuceneTypeIndex>();
    private final BaseLuceneConfiguration _luceneConfiguration;

    public BaseLuceneQueryExtensionManager(QueryExtensionProvider provider, QueryExtensionRuntimeInfo info, BaseLuceneConfiguration configuration) {
        super(info);
        _namespace = provider.getNamespace();
        _luceneConfiguration = configuration;
        File location = new File(_luceneConfiguration.getLocation());
        FileUtils.deleteFileOrDirectoryIfExists(location);
    }

    protected String getNamespace() {
        return _namespace;
    }

    @Override
    public void close() throws IOException {
        for (BaseLuceneTypeIndex luceneHolder : _luceneHolderMap.values())
            luceneHolder.close();

        _luceneHolderMap.clear();
        FileUtils.deleteFileOrDirectoryIfExists(new File(_luceneConfiguration.getLocation()));
        super.close();
    }

    @Override
    public boolean insertEntry(SpaceServerEntry entry, boolean hasPrevious) {
        final String typeName = entry.getSpaceTypeDescriptor().getTypeName();
        final BaseLuceneTypeIndex luceneHolder = _luceneHolderMap.get(typeName);
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

    protected Document createDocumentIfNeeded(BaseLuceneTypeIndex luceneHolder, SpaceServerEntry entry) {

        Document doc = null;
        for (String path : luceneHolder.getQueryExtensionInfo().getPaths()) {
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

    protected abstract Field[] convertField(String path, Object fieldValue);

    @Override
    public void registerType(SpaceTypeDescriptor typeDescriptor) {
        super.registerType(typeDescriptor);
        final String typeName = typeDescriptor.getTypeName();
        if (!_luceneHolderMap.containsKey(typeName)) {
            try {
                _luceneHolderMap.put(typeName, createTypeIndex(_luceneConfiguration, _namespace, typeDescriptor));
            } catch (IOException e) {
                throw new SpaceRuntimeException("Failed to register type " + typeName, e);
            }
        } else {
            _logger.log(Level.WARNING, "Type [" + typeName + "] is already registered");
        }
    }

    protected abstract BaseLuceneTypeIndex createTypeIndex(BaseLuceneConfiguration luceneConfig, String namespace, SpaceTypeDescriptor typeDescriptor) throws IOException;

    @Override
    public QueryExtensionEntryIterator queryByIndex(String typeName, String path, String operationName, Object operand) {
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "query [typeName=" + typeName + ", path=" + path + ", operation=" + operationName + ", operand=" + operand + "]");

        final Query query = createQuery(typeName, path, operationName, operand);
        final BaseLuceneTypeIndex luceneHolder = _luceneHolderMap.get(typeName);
        try {
            // Flush
            luceneHolder.commit(true); //TODO investigate why do we need to commit here

            DirectoryReader dr = DirectoryReader.open(luceneHolder.getDirectory());
            IndexSearcher is = new IndexSearcher(dr);
            ScoreDoc[] scores = is.search(query, MAX_RESULTS).scoreDocs;
            return new LuceneQueryExtensionEntryIterator(scores, is, dr);
        } catch (IOException e) {
            throw new SpaceRuntimeException("Failed to scan index", e);
        }
    }

    protected abstract Query createQuery(String typeName, String path, String operationName, Object operand);

    @Override
    public void removeEntry(SpaceTypeDescriptor typeDescriptor, String uid, int version) {
        final String typeName = typeDescriptor.getTypeName();
        final BaseLuceneTypeIndex luceneHolder = _luceneHolderMap.get(typeName);
        try {
            luceneHolder.getIndexWriter().deleteDocuments(new TermQuery(new Term(XAP_ID_VERSION, concat(uid, version))));
            luceneHolder.commit(false);
        } catch (IOException e) {
            throw new SpaceRuntimeException("Failed to remove entry of type " + typeName, e);
        }
    }


}
