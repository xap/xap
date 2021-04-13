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
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;

import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.bbox.BBoxStrategy;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * @author Yohana Khoury
 * @since 11.0
 */
public class LuceneSpatialConfiguration {
    public static final String FILE_SEPARATOR = File.separator;

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
    public static final String STORAGE_DIRECTORYTYPE_DEFAULT = SupportedDirectory.MMapDirectory.name();
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

    private final SpatialContext _spatialContext;
    private final StrategyFactory _strategyFactory;
    private final DirectoryFactory _directoryFactory;
    private final int _maxUncommittedChanges;
    private final String _location;
    private final boolean needRematch;

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
        this._spatialContext = createSpatialContext(provider);
        this._strategyFactory = createStrategyFactory(provider);
        this._directoryFactory = createDirectoryFactory(provider);
        this._location = initLocation(provider, info);
        //TODO: read from config
        this._maxUncommittedChanges = 1000;
        this.needRematch = Double.parseDouble(provider.getCustomProperty(DIST_ERR_PCT, DIST_ERR_PCT_DEFAULT)) == 0;
    }

    boolean getMatch (){
        return this.needRematch;
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
                        return BBoxStrategy.newInstance(_spatialContext, fieldName);
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

    private static String initLocation(LuceneSpatialQueryExtensionProvider provider, QueryExtensionRuntimeInfo info) {
        //try lucene.storage.location first, if not configured then use workingDir.
        //If workingDir == null (Embedded space , Integrated PU , etc...) then use process working dir (user.dir)
        String location = provider.getCustomProperty(STORAGE_LOCATION, null);
        if (location == null) {
            location = info.getSpaceInstanceWorkDirectory();
            if (location == null)
                location = System.getProperty("user.dir") + FILE_SEPARATOR + "xap";
            location += FILE_SEPARATOR + "spatial";
        }
        String spaceInstanceName = info.getSpaceInstanceName().replace(".", "-");
        return location + FILE_SEPARATOR + spaceInstanceName;
    }

    protected DirectoryFactory createDirectoryFactory(LuceneSpatialQueryExtensionProvider provider) {
        String directoryType = provider.getCustomProperty(STORAGE_DIRECTORYTYPE, STORAGE_DIRECTORYTYPE_DEFAULT);
        SupportedDirectory directory = SupportedDirectory.byName(directoryType);

        switch (directory) {
            case MMapDirectory: {
                return new DirectoryFactory() {
                    @Override
                    public Directory getDirectory(String relativePath) throws IOException {
                        return new MMapDirectory(Paths.get(_location + FILE_SEPARATOR + relativePath));
                    }
                };
            }
            case RAMDirectory: {
                return new DirectoryFactory() {
                    @Override
                    public Directory getDirectory(String path) throws IOException {
                        return new RAMDirectory();
                    }
                };
            }
            default:
                throw new RuntimeException("Unhandled directory type " + directory);
        }
    }


    public SpatialStrategy getStrategy(String fieldName) {
        return this._strategyFactory.createStrategy(fieldName);
    }

    public Directory getDirectory(String relativePath) throws IOException {
        return _directoryFactory.getDirectory(relativePath);
    }

    public SpatialContext getSpatialContext() {
        return _spatialContext;
    }


    public int getMaxUncommittedChanges() {
        return _maxUncommittedChanges;
    }

    public String getLocation() {
        return _location;
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

    public abstract class DirectoryFactory {
        public abstract Directory getDirectory(String relativePath) throws IOException;
    }




}
