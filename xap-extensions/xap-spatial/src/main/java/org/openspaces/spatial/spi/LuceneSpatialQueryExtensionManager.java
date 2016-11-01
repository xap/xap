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
import com.gigaspaces.query.extension.QueryExtensionEntryIterator;
import com.gigaspaces.query.extension.QueryExtensionManager;
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
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.openspaces.spatial.lucene.common.BaseLuceneConfiguration;
import org.openspaces.spatial.lucene.common.BaseLuceneQueryExtensionManager;
import org.openspaces.spatial.lucene.common.BaseLuceneTypeIndex;
import org.openspaces.spatial.shapes.Shape;
import org.openspaces.spatial.spatial4j.Spatial4jShapeProvider;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author yechielf
 * @since 11.0
 */
public class LuceneSpatialQueryExtensionManager extends BaseLuceneQueryExtensionManager<LuceneSpatialConfiguration, LuceneSpatialTypeIndex> {
    private static final Logger _logger = Logger.getLogger(LuceneSpatialQueryExtensionManager.class.getName());

    private static final Map<String, SpatialOperation> _spatialOperations = initSpatialOperations();

    public LuceneSpatialQueryExtensionManager(LuceneSpatialQueryExtensionProvider provider, QueryExtensionRuntimeInfo info, LuceneSpatialConfiguration configuration) {
        super(provider, info, configuration);
    }

    @Override
    protected LuceneSpatialTypeIndex createTypeIndex(LuceneSpatialConfiguration luceneConfig, String namespace, SpaceTypeDescriptor typeDescriptor) throws IOException {
        return new LuceneSpatialTypeIndex(luceneConfig, namespace, typeDescriptor);
    }

    @Override
    protected Query createQuery(String typeName, String path, String operationName, Object operand) {
        final SpatialStrategy spatialStrategy = _luceneConfiguration.getStrategy(path);
        return spatialStrategy.makeQuery(new SpatialArgs(toOperation(operationName), toShape(operand)));
    }

    @Override
    public boolean accept(String typeName, String path, String operationName, Object leftOperand, Object rightOperand) {
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "filter [operation=" + operationName + ", leftOperand=" + leftOperand + ", rightOperand=" + rightOperand + "]");

        return toOperation(operationName).evaluate(toShape(leftOperand), toShape(rightOperand));
    }

    @Override
    protected Field[] convertField(String path, Object fieldValue) {
        if (fieldValue instanceof Shape) {
            final SpatialStrategy strategy = _luceneConfiguration.getStrategy(path);
            return strategy.createIndexableFields(toShape(fieldValue));
        }
        return new Field[0];
    }

    public com.spatial4j.core.shape.Shape toShape(Object obj) {
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

    private static Map<String, SpatialOperation> initSpatialOperations() {
        Map<String, SpatialOperation> result = new HashMap<String, SpatialOperation>();
        result.put("WITHIN", SpatialOperation.IsWithin);
        result.put("CONTAINS", SpatialOperation.Contains);
        result.put("INTERSECTS", SpatialOperation.Intersects);
        return result;
    }
}
