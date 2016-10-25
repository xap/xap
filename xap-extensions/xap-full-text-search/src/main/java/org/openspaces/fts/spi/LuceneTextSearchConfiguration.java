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

import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.SpatialContextFactory;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContextFactory;
import com.spatial4j.core.shape.impl.RectangleImpl;

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
import org.openspaces.spatial.lucene.common.spi.BaseLuceneConfiguration;
import org.openspaces.spatial.lucene.common.spi.BaseLuceneQueryExtensionProvider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * @author Yohana Khoury
 * @since 11.0
 */
public class LuceneTextSearchConfiguration extends BaseLuceneConfiguration {

    public static final String INDEX_LOCATION_FOLDER_NAME = "full_text_search";

    public static final String STORAGE_LOCATION = "lucene.full.text.search.storage.location";
    public static final String MAX_UNCOMMITED_CHANGES = "lucene.full.text.search.max.uncommited.changes";

    public static final String STORAGE_DIRECTORYTYPE = "lucene.full.text.search.storage.directory-type";

    public LuceneTextSearchConfiguration(BaseLuceneQueryExtensionProvider provider, QueryExtensionRuntimeInfo info) {
        super(provider, info);
    }

    @Override
    protected String getMaxUncommitedChangesPropertyKey() {
        return MAX_UNCOMMITED_CHANGES;
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
}
