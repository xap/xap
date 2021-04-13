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

import com.gigaspaces.SpaceRuntimeException;
import com.gigaspaces.internal.io.FileUtils;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.query.extension.QueryExtensionEntryIterator;
import com.gigaspaces.query.extension.QueryExtensionManager;
import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.gigaspaces.server.SpaceServerEntry;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.openspaces.spatial.shapes.Shape;
import org.openspaces.spatial.spatial4j.Spatial4jShapeProvider;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yechielf
 * @since 11.0
 */
public class LuceneSpatialQueryExtensionManager extends QueryExtensionManager {
    private static final Logger _logger = LoggerFactory.getLogger(LuceneSpatialQueryExtensionManager.class.getName());

    protected static final String XAP_ID = "XAP_ID";
    private static final FieldType XAP_ID_TYPE = toFieldType(Field.Store.YES, IndexOptions.NONE, true);
    private static final String XAP_ID_VERSION = "XAP_ID_VERSION";
    private static final FieldType XAP_ID_VERSION_TYPE = toFieldType(Field.Store.YES, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, false);
    private static final int MAX_RESULTS = Integer.MAX_VALUE;
    private static final Map<String, SpatialOperation> _spatialOperations = initSpatialOperations();

    private final Map<String, LuceneSpatialTypeIndex> _luceneHolderMap = new ConcurrentHashMap<String, LuceneSpatialTypeIndex>();
    private final String _namespace;
    private final LuceneSpatialConfiguration _luceneConfiguration;

    public LuceneSpatialQueryExtensionManager(LuceneSpatialQueryExtensionProvider provider, QueryExtensionRuntimeInfo info) {
        super(info);
        _namespace = provider.getNamespace();
        _luceneConfiguration = new LuceneSpatialConfiguration(provider, info);
        File location = new File(_luceneConfiguration.getLocation());
        FileUtils.deleteFileOrDirectoryIfExists(location);
    }

    @Override
    public void close() throws IOException {
        for (LuceneSpatialTypeIndex luceneHolder : _luceneHolderMap.values())
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
                _luceneHolderMap.put(typeName, new LuceneSpatialTypeIndex(_luceneConfiguration, _namespace, typeDescriptor));
            } catch (IOException e) {
                throw new SpaceRuntimeException("Failed to register type " + typeName, e);
            }
        } else {
            _logger.warn("Type [" + typeName + "] is already registered");
        }
    }

    @Override
    public boolean insertEntry(SpaceServerEntry entry, boolean hasPrevious) {
        final String typeName = entry.getSpaceTypeDescriptor().getTypeName();
        final LuceneSpatialTypeIndex luceneHolder = _luceneHolderMap.get(typeName);
        try {
            final Document doc = createDocumentIfNeeded(luceneHolder, entry);
            // Add new
            if (doc != null)
                luceneHolder.getIndexWriter().addDocument(doc);
            // Delete old
            if (hasPrevious)
                luceneHolder.getIndexWriter().deleteDocuments(new TermQuery(new Term(XAP_ID_VERSION,
                        concat(entry.getUid(), entry.getVersion() - 1))));
            // Flush
            if (doc != null || hasPrevious)
                luceneHolder.commit(false);
            return doc != null;
        } catch (Exception e) {
            String operation = hasPrevious ? "update" : "insert";
            throw new SpaceRuntimeException("Failed to " + operation + " entry of type " + typeName + " with id [" + entry.getUid() + "]", e);
        }
    }

    @Override
    public void removeEntry(SpaceTypeDescriptor typeDescriptor, String uid, int version) {
        final String typeName = typeDescriptor.getTypeName();
        final LuceneSpatialTypeIndex luceneHolder = _luceneHolderMap.get(typeName);
        try {
            luceneHolder.getIndexWriter().deleteDocuments(new TermQuery(new Term(XAP_ID_VERSION, concat(uid, version))));
            luceneHolder.commit(false);
        } catch (IOException e) {
            throw new SpaceRuntimeException("Failed to remove entry of type " + typeName, e);
        }
    }

    @Override
    public QueryExtensionEntryIterator queryByIndex(String typeName, String path, String operationName, Object operand) {
        if (_logger.isDebugEnabled())
            _logger.debug("query [typeName=" + typeName + ", path=" + path + ", operation=" + operationName + ", operand=" + operand + "]");

        final SpatialStrategy spatialStrategy = _luceneConfiguration.getStrategy(path);
        final Query query = spatialStrategy.makeQuery(new SpatialArgs(toOperation(operationName), toShape(operand)));
        final LuceneSpatialTypeIndex luceneHolder = _luceneHolderMap.get(typeName);
        try {
            // Flush
            luceneHolder.commit(true);

            DirectoryReader dr = DirectoryReader.open(luceneHolder.getDirectory());
            IndexSearcher is = new IndexSearcher(dr);
            ScoreDoc[] scores = is.search(query, MAX_RESULTS).scoreDocs;
            return new LuceneSpatialQueryExtensionEntryIterator(scores, is, dr, _luceneConfiguration.getMatch());
        } catch (IOException e) {
            throw new SpaceRuntimeException("Failed to scan index", e);
        }
    }

    @Override
    public boolean accept(String typeName, String path, String operation, Object leftOperand, Object rightOperand) {
        if (_logger.isDebugEnabled())
            _logger.debug("filter [operation=" + operation + ", leftOperand=" + leftOperand + ", rightOperand=" + rightOperand + "]");

        return toOperation(operation).evaluate(toShape(leftOperand), toShape(rightOperand));
    }

    protected Document createDocumentIfNeeded(LuceneSpatialTypeIndex luceneHolder, SpaceServerEntry entry) {

        Document doc = null;
        for (String path : luceneHolder.getQueryExtensionInfo().getPaths()) {
            final Object fieldValue = entry.getPathValue(path);
            if (fieldValue instanceof Shape) {
                final SpatialStrategy strategy = _luceneConfiguration.getStrategy(path);
                final Field[] fields = strategy.createIndexableFields(toShape(fieldValue));
                if (doc == null)
                    doc = new Document();
                for (Field field : fields)
                    doc.add(field);
            }
        }
        if (doc != null) {
            //cater for uid & version
            doc.add(new Field(XAP_ID, entry.getUid(), XAP_ID_TYPE));
            doc.add(new Field(XAP_ID_VERSION, concat(entry.getUid(), entry.getVersion()), XAP_ID_VERSION_TYPE));
        }

        return doc;
    }

    public org.locationtech.spatial4j.shape.Shape toShape(Object obj) {
        if (obj instanceof Spatial4jShapeProvider)
            return ((Spatial4jShapeProvider) obj).getSpatial4jShape(_luceneConfiguration.getSpatialContext());
        throw new IllegalArgumentException("Unsupported shape [" + obj.getClass().getName() + "]");
    }

    protected SpatialOperation toOperation(String operationName) {
        SpatialOperation result = _spatialOperations.get(operationName.toUpperCase());
        if (result == null)
            throw new IllegalArgumentException("Operation " + operationName + " not found - supported operations: " + _spatialOperations.keySet());
        return result;
    }

    protected String concat(String uid, int version) {
        return uid + "_" + version;
    }

    private static Map<String, SpatialOperation> initSpatialOperations() {
        Map<String, SpatialOperation> result = new HashMap<String, SpatialOperation>();
        result.put("WITHIN", SpatialOperation.IsWithin);
        result.put("CONTAINS", SpatialOperation.Contains);
        result.put("INTERSECTS", SpatialOperation.Intersects);
        return result;
    }


    private static FieldType toFieldType(Field.Store store, IndexOptions index, boolean analyzed) {
        final FieldType ft = new FieldType();

        ft.setStored(store == Field.Store.YES);
        ft.setIndexOptions(index);
        ft.setTokenized(analyzed);

        ft.freeze();
        return ft;
    }

    @Override
    public Map<String, LongCounter> getIndexedPathsUsageCounters(String typeName) {
        return _luceneHolderMap.get(typeName).getIndexMap();
    }
}
