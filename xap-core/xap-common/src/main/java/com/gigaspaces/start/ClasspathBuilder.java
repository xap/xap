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

import com.gigaspaces.internal.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author Niv Ingberg
 */
@com.gigaspaces.api.InternalApi
public class ClasspathBuilder {

    private static final SystemLocations locations = SystemLocations.singleton();
    private static final Predicate<Path> jarsFilter = FileUtils.Filters.nameEndsWith(".jar");

    private final List<Path> files = new ArrayList<>();

    public ClasspathBuilder appendAny(Path path) {
        return append(path, p -> true);
    }

    public ClasspathBuilder appendJars(Path path) {
        return append(path, jarsFilter);
    }

    public ClasspathBuilder appendJars(Path path, Predicate<Path> filter) {
        return append(path, jarsFilter.and(filter));
    }

    public ClasspathBuilder appendJar(XapModules module) {
        return appendJars(locations.lib(module));
    }

    public ClasspathBuilder appendLibRequiredJars(ClassLoaderType clType) {
        for (Path path : locations.libRequired(clType))
            appendJars(path);
        return this;
    }

    public ClasspathBuilder appendPlatformJars(String subpath) {
        return appendJars(locations.libPlatform(subpath));
    }

    public ClasspathBuilder appendOptionalJars(String subpath) {
        return appendJars(locations.libOptional(subpath));
    }

    public ClasspathBuilder appendOptionalJars(String subpath, Predicate<Path> filter) {
        return appendJars(locations.libOptional(subpath), filter);
    }

    private ClasspathBuilder append(Path path, Predicate<Path> filter) {
        if (Files.isDirectory(path)) {
            try {
                FileUtils.forEach(path, filter, files::add);
            } catch (IOException e) {
                throw new RuntimeException("Failed to append files from " + path, e);
            }
        } else {
            if (filter.test(path))
                files.add(path);
        }
        return this;
    }

    public List<URL> toURLs() throws MalformedURLException {
        List<URL> result = new ArrayList<>();
        for (Path path : files)
            result.add(path.toFile().toURI().toURL());
        return result;
    }

    public URL[] toURLsArray() throws MalformedURLException {
        List<URL> urls = toURLs();
        return urls.toArray(new URL[0]);
    }

    public List<String> toFilesNames() {
        List<String> result = new ArrayList<>();
        for (Path path : files)
            result.add(path.toAbsolutePath().toString());
        return result;
    }

    @Override
    public String toString() {
        return String.join(File.pathSeparator, toFilesNames());
    }
}
