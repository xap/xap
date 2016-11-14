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

package org.openspaces.spatial.lucene.common;

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.metadata.TypeQueryExtension;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BaseLuceneTypeIndex implements Closeable {
    private final Directory directory;
    private final IndexWriter indexWriter;
    private final TypeQueryExtension queryExtensionInfo;
    private final int maxUncommittedChanges;
    private final AtomicInteger uncommittedChanges = new AtomicInteger(0);
    protected final BaseLuceneConfiguration luceneConfig;

    public BaseLuceneTypeIndex(BaseLuceneConfiguration luceneConfig, String namespace, SpaceTypeDescriptor typeDescriptor) throws IOException {
        this.luceneConfig = luceneConfig;
        this.directory = luceneConfig.getDirectory(typeDescriptor.getTypeName() + File.separator + "entries");
        this.indexWriter = new IndexWriter(directory, new IndexWriterConfig(createAnalyzer(luceneConfig, typeDescriptor))
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE));
        this.queryExtensionInfo = typeDescriptor.getQueryExtensions().getByNamespace(namespace);
        this.maxUncommittedChanges = luceneConfig.getMaxUncommittedChanges();
    }

    protected abstract Analyzer createAnalyzer(BaseLuceneConfiguration luceneConfig, SpaceTypeDescriptor typeDescriptor);

    @Override
    public void close() throws IOException {
        indexWriter.close();
    }

    public IndexWriter getIndexWriter() {
        return indexWriter;
    }

    public Directory getDirectory() {
        return directory;
    }

    public TypeQueryExtension getQueryExtensionInfo() {
        return queryExtensionInfo;
    }

    public void commit(boolean force) throws IOException {
        if (force || uncommittedChanges.incrementAndGet() == maxUncommittedChanges) {
            uncommittedChanges.set(0);
            indexWriter.commit();
        }
    }
}
