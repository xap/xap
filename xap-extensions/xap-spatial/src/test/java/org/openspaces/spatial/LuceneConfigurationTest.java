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

package org.openspaces.spatial;

import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;

import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.bbox.BBoxStrategy;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openspaces.spatial.spi.LuceneSpatialConfiguration;
import org.openspaces.spatial.spi.LuceneSpatialQueryExtensionProvider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * @author Yohana Khoury
 * @since 11.0
 */
public class LuceneConfigurationTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @After
    public void tearDown() throws IOException {
        File luceneIndexFolder = new File(System.getProperty("user.dir") + "/luceneIndex");
        if (luceneIndexFolder.exists()) {
            deleteFileOrDirectory(luceneIndexFolder);
        }
    }

    public static void deleteFileOrDirectory(File fileOrDirectory) {
        if (fileOrDirectory.isDirectory()) {
            for (File file : fileOrDirectory.listFiles())
                deleteFileOrDirectory(file);
        }
        if (!fileOrDirectory.delete()) {
            throw new RuntimeException("Failed to delete " + fileOrDirectory);
        }
    }

    private String getWorkingDir() {
        return temporaryFolder.getRoot().getAbsolutePath();
    }

    @Test
    public void testDefaults() throws IOException {
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());
        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(new LuceneSpatialQueryExtensionProvider(), config);

        //test directory
        Directory directory = luceneConfiguration.getDirectory("A");
        Assert.assertEquals("MMapDirectory should be the default directory", MMapDirectory.class, directory.getClass());
        String expectedLocation = getWorkingDir() + File.separator + "spatial" + File.separator + config.getSpaceInstanceName();
        Assert.assertEquals("Default location", expectedLocation, luceneConfiguration.getLocation());
        Assert.assertEquals("MMapDirectory location should be workingDir/luceneIndex/A " + expectedLocation + "/A", new MMapDirectory(Paths.get(expectedLocation + "/A")).getDirectory(), ((MMapDirectory) directory).getDirectory());

        //test strategy
        SpatialStrategy strategy = luceneConfiguration.getStrategy("myfield");
        Assert.assertEquals("Default strategy should be RecursivePrefixTree", RecursivePrefixTreeStrategy.class, strategy.getClass());
        Assert.assertEquals("Unexpected spatial context", JtsSpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertTrue("DistErrPct default for strategy should be 0.025", 0.025 == ((RecursivePrefixTreeStrategy) strategy).getDistErrPct());

        //test spatialprefixtree
        Assert.assertEquals("GeohashPrefixTree should be the default spatial prefix tree for strategy", GeohashPrefixTree.class, ((RecursivePrefixTreeStrategy) strategy).getGrid().getClass());
        Assert.assertEquals("MaxLevels should be 11 as default", 11, ((RecursivePrefixTreeStrategy) strategy).getGrid().getMaxLevels());

        //test spatialcontext
        Assert.assertEquals("Default spatialcontext should be JTS", JtsSpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Default spatialcontext.geo should be true", true, luceneConfiguration.getSpatialContext().isGeo());

    }

    @Test
    public void testInvalidDirectoryType() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.storage.directory-type", "A");
        try {
            new LuceneSpatialConfiguration(provider, new MockConfig());
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("Unsupported directory: A - supported values: [MMapDirectory, RAMDirectory]", e.getMessage());
        }
    }

    @Test
    public void testRAMDirectoryTypeCaseInsensitive() throws IOException {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.storage.directory-type", "Ramdirectory");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(provider, config);

        Directory directory = luceneConfiguration.getDirectory("unused");
        Assert.assertEquals("Unexpected Directory type", RAMDirectory.class, directory.getClass());
    }

    @Test
    public void testMMapDirectoryTypeAndLocation() throws IOException {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.storage.directory-type", "MMapDirectory")
                .setCustomProperty("lucene.storage.location", temporaryFolder.getRoot().getAbsolutePath() + "/tempdir");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(provider, config);
        Directory directory = luceneConfiguration.getDirectory("subfolder");

        Assert.assertEquals("Unexpected Directory type", MMapDirectory.class, directory.getClass());
        Assert.assertEquals(temporaryFolder.getRoot().getAbsolutePath()
                        + File.separator + "tempdir"
                        + File.separator + config.getSpaceInstanceName()
                        + File.separator + "subfolder",
                ((MMapDirectory) directory).getDirectory().toFile().getAbsolutePath());
    }

    @Test
    public void testLocationNoWorkingDir() throws IOException {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.storage.directory-type", "MMapDirectory");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(null); //null as second parameter simulates there is no working dir (not pu)

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(provider, config);
        Directory directory = luceneConfiguration.getDirectory("subfolder");

        Assert.assertEquals("Unexpected Directory type", MMapDirectory.class, directory.getClass());
        Assert.assertEquals(System.getProperty("user.dir") +
                        File.separator + "xap" +
                        File.separator + "spatial" +
                        File.separator + config.getSpaceInstanceName() +
                        File.separator + "subfolder",
                ((MMapDirectory) directory).getDirectory().toFile().getAbsolutePath());
    }

    @Test
    public void testInvalidSpatialContextTree() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.strategy.spatial-prefix-tree", "invalidValue");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());
        try {
            new LuceneSpatialConfiguration(provider, config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("Unsupported spatial prefix tree: invalidValue - supported values: [GeohashPrefixTree, QuadPrefixTree]", e.getMessage());
        }
    }

    @Test
    public void testSpatialContextTreeQuadPrefixTreeAndMaxLevels() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.strategy.spatial-prefix-tree", "QuadPrefixTree")
                .setCustomProperty("lucene.strategy.spatial-prefix-tree.max-levels", "20");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(provider, config);

        SpatialStrategy strategy = luceneConfiguration.getStrategy("myField");
        Assert.assertEquals("Unexpected spatial prefix tree", QuadPrefixTree.class, ((RecursivePrefixTreeStrategy) strategy).getGrid().getClass());
        Assert.assertEquals("MaxLevels should be 20", 20, ((RecursivePrefixTreeStrategy) strategy).getGrid().getMaxLevels());
    }

    @Test
    public void testInvalidSpatialContext() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("context", "dummy");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());

        try {
            new LuceneSpatialConfiguration(provider, config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("Unsupported spatial context: dummy - supported values: [Spatial4J, JTS]", e.getMessage());
        }
    }

    @Test
    public void testSpatialContextGEO() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("context", "spatial4j")
                .setCustomProperty("context.geo", "true");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(provider, config);

        Assert.assertEquals("Unexpected spatial context", SpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Expecting geo spatial context", true, luceneConfiguration.getSpatialContext().isGeo());
    }

    @Test
    public void testSpatialContextNonGEO() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.strategy.spatial-prefix-tree", "QuadPrefixTree")
                .setCustomProperty("context", "spatial4j")
                .setCustomProperty("context.geo", "false");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(provider, config);

        Assert.assertEquals("Unexpected spatial context", SpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Expecting geo spatial context", false, luceneConfiguration.getSpatialContext().isGeo());
    }

    @Test
    public void testSpatialContextJTSNonGEO() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.strategy.spatial-prefix-tree", "QuadPrefixTree")
                .setCustomProperty("context", "jts")
                .setCustomProperty("context.geo", "false");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(provider, config);

        Assert.assertEquals("Unexpected spatial context", JtsSpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Expecting geo spatial context", false, luceneConfiguration.getSpatialContext().isGeo());
    }

    @Test
    public void testSpatialContextGEOInvalidWorldBoundsPropertyValue() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("context", "jts")
                .setCustomProperty("context.geo", "true")
                .setCustomProperty("context.world-bounds", "invalidvaluehere");

        QueryExtensionRuntimeInfo config = new MockConfig();
        try {
            new LuceneSpatialConfiguration(provider, config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("World bounds [invalidvaluehere] must be of format: minX, maxX, minY, maxY", e.getMessage());
        }
    }

    @Test
    public void testSpatialContextNONGEOInvalidWorldBoundsValues() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("context", "spatial4J")
                .setCustomProperty("context.geo", "false")
                .setCustomProperty("context.world-bounds", "1,7,9,1");
        QueryExtensionRuntimeInfo config = new MockConfig();

        try {
            new LuceneSpatialConfiguration(provider, config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("Values of world bounds [minX, maxX, minY, maxY]=[1,7,9,1] must meet: minX<=maxX, minY<=maxY", e.getMessage());
        }
    }

    @Test
    public void testSpatialContextJTSGEOInvalidWorldBoundsStringValue() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("context", "jts")
                .setCustomProperty("context.geo", "true")
                .setCustomProperty("context.world-bounds", "1,7,1,4a");
        QueryExtensionRuntimeInfo config = new MockConfig();

        try {
            new LuceneSpatialConfiguration(provider, config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("Invalid world bounds [1,7,1,4a] - token #4 is not a number", e.getMessage());
        }
    }

    @Test
    public void testSpatialContextJTSNONGEO() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.strategy.spatial-prefix-tree", "QuadPrefixTree")
                .setCustomProperty("context", "jts")
                .setCustomProperty("context.geo", "false")
                .setCustomProperty("context.world-bounds", "1,10,-100,100");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(provider, config);

        Assert.assertEquals("Unexpected spatial context", JtsSpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Expecting geo spatial context", false, luceneConfiguration.getSpatialContext().isGeo());
        Assert.assertEquals("Unexpected spatial context world bound", new RectangleImpl(1, 10, -100, 100, null), luceneConfiguration.getSpatialContext().getWorldBounds());
    }

    @Test
    public void testInvalidStrategy() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.strategy", "mystrategy");
        QueryExtensionRuntimeInfo config = new MockConfig();
        try {
            new LuceneSpatialConfiguration(provider, config);
            Assert.fail("An exception should be thrown");
        } catch (RuntimeException e) {
            //OK
            Assert.assertEquals("Unsupported Spatial strategy: mystrategy - supported values: [RecursivePrefixTree, BBox, Composite]", e.getMessage());
        }
    }

    @Test
    public void testStrategyRecursivePrefixTreeAndDistErrPct() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.strategy", "RecursivePrefixTree")
                .setCustomProperty("lucene.strategy.spatial-prefix-tree", "GeohashPrefixTree")
                .setCustomProperty("lucene.strategy.spatial-prefix-tree.max-levels", "10")
                .setCustomProperty("lucene.strategy.distance-error-pct", "0.5");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(provider, config);

        Assert.assertEquals("Unexpected strategy type", RecursivePrefixTreeStrategy.class, luceneConfiguration.getStrategy("myField").getClass());
        RecursivePrefixTreeStrategy strategy = (RecursivePrefixTreeStrategy) luceneConfiguration.getStrategy("myField");
        Assert.assertEquals("Unexpected spatial prefix tree", GeohashPrefixTree.class, strategy.getGrid().getClass());
        Assert.assertEquals("MaxLevels should be 10", 10, strategy.getGrid().getMaxLevels());
        Assert.assertTrue("Expecting distance-error-pct to be 0.5", 0.5 == strategy.getDistErrPct());
    }

    @Test
    public void testStrategyBBox() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.strategy", "BBox")
                .setCustomProperty("context", "spatial4J")
                .setCustomProperty("context.geo", "false");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(provider, config);

        Assert.assertEquals("Unexpected strategy type", BBoxStrategy.class, luceneConfiguration.getStrategy("myField").getClass());
        Assert.assertEquals("Unexpected spatial context", SpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Expecting geo spatial context", false, luceneConfiguration.getSpatialContext().isGeo());
    }

    @Test
    public void testStrategyComposite() {
        LuceneSpatialQueryExtensionProvider provider = new LuceneSpatialQueryExtensionProvider()
                .setCustomProperty("lucene.strategy", "composite")
                .setCustomProperty("context", "spatial4J")
                .setCustomProperty("context.geo", "true");
        QueryExtensionRuntimeInfo config = new MockConfig()
                .setWorkDir(getWorkingDir());

        LuceneSpatialConfiguration luceneConfiguration = new LuceneSpatialConfiguration(provider, config);

        Assert.assertEquals("Unexpected strategy type", CompositeSpatialStrategy.class, luceneConfiguration.getStrategy("myField").getClass());
        Assert.assertEquals("Unexpected spatial context", SpatialContext.class, luceneConfiguration.getSpatialContext().getClass());
        Assert.assertEquals("Expecting geo spatial context", true, luceneConfiguration.getSpatialContext().isGeo());
    }

    private static class MockConfig implements QueryExtensionRuntimeInfo {
        private String workDir;

        @Override
        public String getSpaceInstanceName() {
            return "foo";
        }

        @Override
        public String getSpaceInstanceWorkDirectory() {
            return workDir;
        }

        public MockConfig setWorkDir(String workDir) {
            this.workDir = workDir;
            return this;
        }
    }
}