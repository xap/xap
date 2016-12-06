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

import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * @author Vitaliy_Zinchenko
 * @since 12.1
 */
public class LuceneTextSearchConfiguration {

    public static final String INDEX_LOCATION_FOLDER_NAME = "full_text_search";

    public static final String FILE_SEPARATOR = File.separator;

    public static final String STORAGE_LOCATION = "lucene.storage.location";

    //lucene.storage.directory-type
    public static final String STORAGE_DIRECTORY_TYPE = "lucene.storage.directory-type";
    public static final String STORAGE_DIRECTORYTYPE_DEFAULT = SupportedDirectory.MMapDirectory.name();

    public static final String MAX_UNCOMMITED_CHANGES = "lucene.max-uncommitted-changes";
    public static final String DEFAULT_MAX_UNCOMMITED_CHANGES = "1000";

    public static final String MAX_RESULTS = "lucene.max-results";
    private static final String DEFAULT_MAX_RESULTS = String.valueOf(Integer.MAX_VALUE);

    public static final Class<StandardAnalyzer> DEFAULT_ANALYZER_CLASS = StandardAnalyzer.class;

    private final DirectoryFactory _directoryFactory;
    private final int _maxUncommittedChanges;
    private final String _location;
    private final int _maxResults;
    private final Analyzer _defaultAnalyzer;

    public LuceneTextSearchConfiguration(LuceneTextSearchQueryExtensionProvider provider, QueryExtensionRuntimeInfo info) {
        this._directoryFactory = createDirectoryFactory(provider);
        this._location = initLocation(provider, info);
        this._maxUncommittedChanges = initMaxUncommittedChanges(provider);
        this._maxResults = initMaxResults(provider);
        this._defaultAnalyzer = initDefaultAnalyzer();
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

    private int initMaxUncommittedChanges(LuceneTextSearchQueryExtensionProvider provider) {
        return Integer.parseInt(provider.getCustomProperty(MAX_UNCOMMITED_CHANGES, DEFAULT_MAX_UNCOMMITED_CHANGES));
    }

    private int initMaxResults(LuceneTextSearchQueryExtensionProvider provider) {
        return Integer.parseInt(provider.getCustomProperty(MAX_RESULTS, DEFAULT_MAX_RESULTS));
    }

    private Analyzer initDefaultAnalyzer() {
        return createAnalyzer(DEFAULT_ANALYZER_CLASS);
    }

    private String initLocation(LuceneTextSearchQueryExtensionProvider provider, QueryExtensionRuntimeInfo info) {
        //try lucene.storage.location first, if not configured then use workingDir.
        //If workingDir == null (Embedded space , Integrated PU , etc...) then use process working dir (user.dir)
        String location = provider.getCustomProperty(STORAGE_LOCATION, null);
        if (location == null) {
            location = info.getSpaceInstanceWorkDirectory();
            if (location == null)
                location = System.getProperty("user.dir") + FILE_SEPARATOR + "xap";
            location += FILE_SEPARATOR + INDEX_LOCATION_FOLDER_NAME;
        }
        String spaceInstanceName = info.getSpaceInstanceName().replace(".", "-");
        return location + FILE_SEPARATOR + spaceInstanceName;
    }

    protected DirectoryFactory createDirectoryFactory(LuceneTextSearchQueryExtensionProvider provider) {
        String directoryType = provider.getCustomProperty(STORAGE_DIRECTORY_TYPE, STORAGE_DIRECTORYTYPE_DEFAULT);
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

    public Analyzer getDefaultAnalyzer() {
        return _defaultAnalyzer;
    }

    public int getMaxResults() {
        return _maxResults;
    }

    public static Analyzer createAnalyzer(Class analyzerClass) {
        try {
            return (Analyzer) analyzerClass.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to instantiate analyzer " + analyzerClass, e);
        }
    }

}
