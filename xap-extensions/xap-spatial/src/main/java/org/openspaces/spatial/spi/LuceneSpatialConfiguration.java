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

import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.SpatialContextFactory;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContextFactory;
import com.spatial4j.core.shape.impl.RectangleImpl;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.bbox.BBoxStrategy;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.openspaces.spatial.lucene.common.BaseLuceneConfiguration;

import java.io.File;
import java.util.Arrays;

/**
 * @author Yohana Khoury
 * @since 11.0
 */
public class LuceneSpatialConfiguration extends BaseLuceneConfiguration {

    //lucene.strategy
    public static final String STRATEGY = "lucene.strategy";
    public static final String STRATEGY_DEFAULT = SupportedSpatialStrategy.RecursivePrefixTree.name();

    //lucene.strategy.spatial-prefix-tree
    public static final String SPATIAL_PREFIX_TREE = "lucene.strategy.spatial-prefix-tree";
    public static final String SPATIAL_PREFIX_TREE_DEFAULT = SupportedSpatialPrefixTree.GeohashPrefixTree.name();
    //lucene.strategy.spatial-prefix-tree.max-levels
    public static final String SPATIAL_PREFIX_TREE_MAX_LEVELS = "lucene.strategy.spatial-prefix-tree.max-levels";
    public static final String SPATIAL_PREFIX_TREE_MAX_LEVELS_DEFAULT = "11";
    //lucene.strategy.dist-err-pct
    public static final String DIST_ERR_PCT = "lucene.strategy.distance-error-pct";
    public static final String DIST_ERR_PCT_DEFAULT = "0.025";

    //lucene.storage.directory-type
    public static final String STORAGE_DIRECTORYTYPE = "lucene.storage.directory-type";
    //lucene.storage.location
    public static final String STORAGE_LOCATION = "lucene.storage.location";

    //context
    public static final String SPATIAL_CONTEXT = "context";
    public static final String SPATIAL_CONTEXT_DEFAULT = SupportedSpatialContext.JTS.name();

    //context.geo
    public static final String SPATIAL_CONTEXT_GEO = "context.geo";
    public static final String SPATIAL_CONTEXT_GEO_DEFAULT = "true";

    //context.world-bounds, default is set by lucene
    public static final String SPATIAL_CONTEXT_WORLD_BOUNDS = "context.world-bounds";
    public static final String INDEX_LOCATION_FOLDER_NAME = "spatial";

    public static final String MAX_UNCOMMITED_CHANGES = "lucene.spatial.max.uncommited.changes";
    public static final String MAX_RESULTS = "lucene.spatial.max.results";

    private final SpatialContext _spatialContext;
    private final StrategyFactory _strategyFactory;

    private enum SupportedSpatialStrategy {
        RecursivePrefixTree, BBox, Composite;

        public static SupportedSpatialStrategy byName(String key) {
            for (SupportedSpatialStrategy spatialStrategy : SupportedSpatialStrategy.values())
                if (spatialStrategy.name().equalsIgnoreCase(key))
                    return spatialStrategy;

            throw new IllegalArgumentException("Unsupported Spatial strategy: " + key + " - supported values: " + Arrays.asList(values()));
        }
    }

    private enum SupportedSpatialPrefixTree {
        GeohashPrefixTree, QuadPrefixTree;

        public static SupportedSpatialPrefixTree byName(String key) {
            for (SupportedSpatialPrefixTree spatialPrefixTree : SupportedSpatialPrefixTree.values())
                if (spatialPrefixTree.name().equalsIgnoreCase(key))
                    return spatialPrefixTree;


            throw new IllegalArgumentException("Unsupported spatial prefix tree: " + key + " - supported values: " + Arrays.asList(values()));
        }
    }

    private enum SupportedSpatialContext {
        Spatial4J, JTS;

        public static SupportedSpatialContext byName(String key) {
            for (SupportedSpatialContext spatialContext : SupportedSpatialContext.values())
                if (spatialContext.name().equalsIgnoreCase(key))
                    return spatialContext;

            throw new IllegalArgumentException("Unsupported spatial context: " + key + " - supported values: " + Arrays.asList(values()));
        }
    }

    private enum SupportedDirectory {
        MMapDirectory, RAMDirectory;

        public static SupportedDirectory byName(String key) {
            for (SupportedDirectory directory : SupportedDirectory.values())
                if (directory.name().equalsIgnoreCase(key))
                    return directory;

            throw new IllegalArgumentException("Unsupported directory: " + key + " - supported values: " + Arrays.asList(values()));
        }
    }

    public LuceneSpatialConfiguration(LuceneSpatialQueryExtensionProvider provider, QueryExtensionRuntimeInfo info) {
        super(provider, info);
        this._spatialContext = createSpatialContext(provider);
        this._strategyFactory = createStrategyFactory(provider);
    }

    private static RectangleImpl createSpatialContextWorldBounds(LuceneSpatialQueryExtensionProvider provider) {
        String spatialContextWorldBounds = provider.getCustomProperty(SPATIAL_CONTEXT_WORLD_BOUNDS, null);
        if (spatialContextWorldBounds == null)
            return null;

        String[] tokens = spatialContextWorldBounds.split(",");
        if (tokens.length != 4)
            throw new IllegalArgumentException("World bounds [" + spatialContextWorldBounds + "] must be of format: minX, maxX, minY, maxY");
        double[] worldBounds = new double[tokens.length];
        for (int i = 0; i < worldBounds.length; i++) {
            try {
                worldBounds[i] = Double.parseDouble(tokens[i].trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid world bounds [" + spatialContextWorldBounds + "] - token #" + (i + 1) + " is not a number");
            }
        }

        double minX = worldBounds[0];
        double maxX = worldBounds[1];
        double minY = worldBounds[2];
        double maxY = worldBounds[3];
        if (!((minX <= maxX) && (minY <= maxY)))
            throw new IllegalStateException("Values of world bounds [minX, maxX, minY, maxY]=[" + spatialContextWorldBounds + "] must meet: minX<=maxX, minY<=maxY");

        return new RectangleImpl(minX, maxX, minY, maxY, null);
    }

    private static SpatialContext createSpatialContext(LuceneSpatialQueryExtensionProvider provider) {
        String spatialContextString = provider.getCustomProperty(SPATIAL_CONTEXT, SPATIAL_CONTEXT_DEFAULT);
        SupportedSpatialContext spatialContext = SupportedSpatialContext.byName(spatialContextString);
        boolean geo = Boolean.valueOf(provider.getCustomProperty(SPATIAL_CONTEXT_GEO, SPATIAL_CONTEXT_GEO_DEFAULT));
        RectangleImpl worldBounds = createSpatialContextWorldBounds(provider);

        switch (spatialContext) {
            case JTS: {
                JtsSpatialContextFactory factory = new JtsSpatialContextFactory();
                factory.geo = geo;
                if (worldBounds != null)
                    factory.worldBounds = worldBounds;
                return new JtsSpatialContext(factory);
            }
            case Spatial4J: {
                SpatialContextFactory factory = new SpatialContextFactory();
                factory.geo = geo;
                if (worldBounds != null)
                    factory.worldBounds = worldBounds;
                return new SpatialContext(factory);
            }
            default:
                throw new IllegalStateException("Unsupported spatial context type " + spatialContext);
        }
    }

    protected StrategyFactory createStrategyFactory(LuceneSpatialQueryExtensionProvider provider) {
        String strategyString = provider.getCustomProperty(STRATEGY, STRATEGY_DEFAULT);
        SupportedSpatialStrategy spatialStrategy = SupportedSpatialStrategy.byName(strategyString);

        switch (spatialStrategy) {
            case RecursivePrefixTree: {
                final SpatialPrefixTree geohashPrefixTree = createSpatialPrefixTree(provider, _spatialContext);
                String distErrPctValue = provider.getCustomProperty(DIST_ERR_PCT, DIST_ERR_PCT_DEFAULT);
                final double distErrPct = Double.valueOf(distErrPctValue);

                return new StrategyFactory(spatialStrategy) {
                    @Override
                    public SpatialStrategy createStrategy(String fieldName) {
                        RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(geohashPrefixTree, fieldName);
                        strategy.setDistErrPct(distErrPct);
                        return strategy;
                    }
                };
            }
            case BBox: {
                return new StrategyFactory(spatialStrategy) {
                    @Override
                    public SpatialStrategy createStrategy(String fieldName) {
                        return new BBoxStrategy(_spatialContext, fieldName);
                    }
                };
            }
            case Composite: {
                final SpatialPrefixTree geohashPrefixTree = createSpatialPrefixTree(provider, _spatialContext);
                String distErrPctValue = provider.getCustomProperty(DIST_ERR_PCT, DIST_ERR_PCT_DEFAULT);
                final double distErrPct = Double.valueOf(distErrPctValue);

                return new StrategyFactory(spatialStrategy) {
                    @Override
                    public SpatialStrategy createStrategy(String fieldName) {
                        RecursivePrefixTreeStrategy recursivePrefixTreeStrategy = new RecursivePrefixTreeStrategy(geohashPrefixTree, fieldName);
                        recursivePrefixTreeStrategy.setDistErrPct(distErrPct);
                        SerializedDVStrategy serializedDVStrategy = new SerializedDVStrategy(_spatialContext, fieldName);
                        return new CompositeSpatialStrategy(fieldName, recursivePrefixTreeStrategy, serializedDVStrategy);
                    }
                };
            }
            default:
                throw new IllegalStateException("Unsupported strategy: " + spatialStrategy);
        }
    }

    private static SpatialPrefixTree createSpatialPrefixTree(LuceneSpatialQueryExtensionProvider provider, SpatialContext spatialContext) {
        String spatialPrefixTreeType = provider.getCustomProperty(SPATIAL_PREFIX_TREE, SPATIAL_PREFIX_TREE_DEFAULT);

        SupportedSpatialPrefixTree spatialPrefixTree = SupportedSpatialPrefixTree.byName(spatialPrefixTreeType);
        String maxLevelsStr = provider.getCustomProperty(SPATIAL_PREFIX_TREE_MAX_LEVELS, SPATIAL_PREFIX_TREE_MAX_LEVELS_DEFAULT);
        int maxLevels = Integer.valueOf(maxLevelsStr);

        switch (spatialPrefixTree) {
            case GeohashPrefixTree:
                return new GeohashPrefixTree(spatialContext, maxLevels);
            case QuadPrefixTree:
                return new QuadPrefixTree(spatialContext, maxLevels);
            default:
                throw new RuntimeException("Unhandled spatial prefix tree type: " + spatialPrefixTree);
        }
    }

    public SpatialStrategy getStrategy(String fieldName) {
        return this._strategyFactory.createStrategy(fieldName);
    }

    @Override
    protected String getMaxUncommitedChangesPropertyKey() {
        return MAX_UNCOMMITED_CHANGES;
    }

    @Override
    protected String getMaxResultsPropertyKey() {
        return MAX_RESULTS;
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

    public SpatialContext getSpatialContext() {
        return _spatialContext;
    }

    @Override
    public Analyzer getDefaultAnalyzer() {
        return null;
    }

    public abstract class StrategyFactory {
        private SupportedSpatialStrategy _strategyName;

        public StrategyFactory(SupportedSpatialStrategy strategyName) {
            this._strategyName = strategyName;
        }

        public abstract SpatialStrategy createStrategy(String fieldName);

        public SupportedSpatialStrategy getStrategyName() {
            return _strategyName;
        }
    }

}
