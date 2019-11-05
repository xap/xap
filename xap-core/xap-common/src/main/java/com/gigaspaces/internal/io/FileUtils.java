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

package com.gigaspaces.internal.io;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Niv Ingberg
 * @since 9.5.0
 */
@com.gigaspaces.api.InternalApi
public class FileUtils {
    public static File[] findFiles(File folder, final String prefix, final String suffix) {
        return folder.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (!file.isFile())
                    return false;
                final String fileName = file.getName();
                if (prefix != null && !fileName.startsWith(prefix))
                    return false;
                if (suffix != null && !fileName.endsWith(suffix))
                    return false;
                return true;
            }
        });
    }

    public static boolean deleteFileOrDirectoryIfExists(File fileOrDirectory) {
        if (fileOrDirectory.isDirectory()) {
            for (File file : BootIOUtils.listFiles(fileOrDirectory))
                deleteFileOrDirectoryIfExists(file);
        }
        return fileOrDirectory.delete();
    }

    public static Collection<Path> list(Path p) throws IOException {
        try (Stream<Path> stream = Files.list(p)) {
            return stream.collect(Collectors.toList());
        }
    }

    public static Collection<Path> list(Path p, Predicate<Path> filter) throws IOException {
        try (Stream<Path> stream = Files.list(p)) {
            return stream.filter(filter).collect(Collectors.toList());
        }
    }

    public static void forEach(Path p, Consumer<Path> consumer) throws IOException {
        try (Stream<Path> stream = Files.list(p)) {
            stream.forEach(consumer);
        }
    }

    public static void forEach(Path p, Predicate<Path> filter, Consumer<Path> consumer) throws IOException {
        try (Stream<Path> stream = Files.list(p)) {
            stream.filter(filter).forEach(consumer);
        }
    }
}
