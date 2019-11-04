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

package com.gigaspaces.start;

import com.gigaspaces.internal.io.BootIOUtils;

import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;

/**
 * @author Niv Ingberg
 */
@com.gigaspaces.api.InternalApi
public class ClasspathBuilder {

    private final List<File> files = new ArrayList<File>();

    public ClasspathBuilder appendRequired() {
        return appendRequired(null);
    }

    public ClasspathBuilder appendRequired(FileFilter filter) {
        return append(SystemLocations.singleton().libRequired(), filter);
    }

    public ClasspathBuilder appendPlatform(String path, String ... more) {
        return append(SystemLocations.singleton().libPlatform().resolve(Paths.get(path, more)));
    }

    public ClasspathBuilder appendOptional(String path) {
        return appendOptional(path, null);
    }

    public ClasspathBuilder appendOptional(String path, FileFilter filter) {
        return append(SystemLocations.singleton().libOptional(path), filter);
    }

    public ClasspathBuilder append(XapModules module) {
        return append(SystemLocations.singleton().lib(module));
    }

    public ClasspathBuilder append(Path path) {
        return append(path, null);
    }

    public ClasspathBuilder append(Path path, FileFilter filter) {
        return append(path, filter, true);
    }

    public ClasspathBuilder append(Path path, FileFilter filter, boolean archivesOnly) {
        filter = archivesOnly ? new JarFileFilter(filter) : filter;
        File f = path.toFile();
        if (f.isDirectory()) {
            final File[] files = BootIOUtils.listFiles(f, filter);
            for (File file : files)
                this.files.add(file);
        } else {
            if (filter == null || filter.accept(f))
                files.add(f);
        }
        return this;
    }

    public List<URL> toURLs() throws MalformedURLException {
        List<URL> result = new ArrayList<URL>();
        for (File file : files)
            result.add(file.toURI().toURL());
        return result;
    }

    public URL[] toURLsArray() throws MalformedURLException {
        List<URL> urls = toURLs();
        return urls.toArray(new URL[urls.size()]);
    }

    public List<String> toFilesNames() {
        List<String> result = new ArrayList<String>();
        for (File file : files)
            result.add(file.getAbsolutePath());
        return result;
    }

    @Override
    public String toString() {
        return String.join(File.pathSeparator, toFilesNames());
    }

    private static class JarFileFilter implements FileFilter {

        private final FileFilter secondaryFilter;

        private JarFileFilter(FileFilter secondaryFilter) {
            this.secondaryFilter = secondaryFilter;
        }

        @Override
        public boolean accept(File pathname) {
            String filename = pathname.getName().toLowerCase();
            if (filename.endsWith(".jar") || filename.endsWith(".zip")) {
                return secondaryFilter != null ? secondaryFilter.accept(pathname) : true;
            } else {
                return false;
            }
        }
    }

    private static String path(String base, String subdir) {
        return base + File.separator + subdir;
    }

    public static FileFilter startsWithFilter(final String... prefix) {
        return new FileFilter() {
            @Override
            public boolean accept(File file) {
                for (String p : prefix)
                    if (file.getName().startsWith(p))
                        return true;
                return false;
            }
        };
    }
}
