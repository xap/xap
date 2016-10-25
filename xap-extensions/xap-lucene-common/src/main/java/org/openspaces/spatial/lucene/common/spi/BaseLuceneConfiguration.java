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

import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;

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
public abstract class BaseLuceneConfiguration {
    public static final String FILE_SEPARATOR = File.separator;

    //lucene.storage.directory-type
    public static final String STORAGE_DIRECTORYTYPE_DEFAULT = SupportedDirectory.MMapDirectory.name();

    public static final String DEFAULT_MAX_UNCOMMITED_CHANGES = "1000";

    private final DirectoryFactory _directoryFactory;
    private final int _maxUncommittedChanges;
    private final String _location;

    private enum SupportedDirectory {
        MMapDirectory, RAMDirectory;

        public static SupportedDirectory byName(String key) {
            for (SupportedDirectory directory : SupportedDirectory.values())
                if (directory.name().equalsIgnoreCase(key))
                    return directory;

            throw new IllegalArgumentException("Unsupported directory: " + key + " - supported values: " + Arrays.asList(values()));
        }
    }

    public BaseLuceneConfiguration(BaseLuceneQueryExtensionProvider provider, QueryExtensionRuntimeInfo info) {

        this._directoryFactory = createDirectoryFactory(provider);
        this._location = initLocation(provider, info);
        this._maxUncommittedChanges = initMaxUncommittedChanges(provider);
    }

    private int initMaxUncommittedChanges(BaseLuceneQueryExtensionProvider provider) {
        return Integer.parseInt(provider.getCustomProperty(getMaxUncommitedChangesPropertyKey(), DEFAULT_MAX_UNCOMMITED_CHANGES));
    }

    protected abstract String getMaxUncommitedChangesPropertyKey();

    private String initLocation(BaseLuceneQueryExtensionProvider provider, QueryExtensionRuntimeInfo info) {
        //try lucene.storage.location first, if not configured then use workingDir.
        //If workingDir == null (Embedded space , Integrated PU , etc...) then use process working dir (user.dir)
        String location = provider.getCustomProperty(getStorageLocationPropertyKey(), null);
        if (location == null) {
            location = info.getSpaceInstanceWorkDirectory();
            if (location == null)
                location = System.getProperty("user.dir") + FILE_SEPARATOR + "xap";
            location += FILE_SEPARATOR + getIndexLocationFolderName();
        }
        String spaceInstanceName = info.getSpaceInstanceName().replace(".", "-");
        return location + FILE_SEPARATOR + spaceInstanceName;
    }

    protected abstract String getIndexLocationFolderName();

    protected abstract String getStorageLocationPropertyKey();

    protected DirectoryFactory createDirectoryFactory(BaseLuceneQueryExtensionProvider provider) {
        String directoryType = provider.getCustomProperty(getStorageDirectoryTypePropertyKey(), STORAGE_DIRECTORYTYPE_DEFAULT);
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

    protected abstract String getStorageDirectoryTypePropertyKey();

    public Directory getDirectory(String relativePath) throws IOException {
        return _directoryFactory.getDirectory(relativePath);
    }

    public int getMaxUncommittedChanges() {
        return _maxUncommittedChanges;
    }

    public String getLocation() {
        return _location;
    }

    public abstract class DirectoryFactory {
        public abstract Directory getDirectory(String relativePath) throws IOException;
    }

}
