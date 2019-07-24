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
package org.gigaspaces.blueprints;

import com.gigaspaces.internal.jvm.JavaUtils;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Niv Ingberg
 * @since 14.5
 */
public class TemplateUtils {
    private static final MustacheFactory mf = new DefaultMustacheFactory();
    private static final String IOEXCEPTION_WRAPPER = "IOException wrapper";

    public static String evaluate(String text, Map<String, Object> context) {
        Mustache m = mf.compile(new StringReader(text), "temp.name");
        return m.execute(new StringWriter(), context).toString();
    }

    public static void evaluate(Path src, Path dst, Map<String, Object> context) throws IOException {
        if (Files.isDirectory(src)) {
            Files.createDirectories(dst);
        } else {
            try (FileReader reader = new FileReader(src.toFile())) {
                try (FileWriter writer = new FileWriter(dst.toFile())) {
                    mf.compile(reader, "temp.name").execute(writer, context);
                }
            }
            if(!JavaUtils.isWindows()) {
                Files.setPosixFilePermissions(dst, Files.getPosixFilePermissions(src));
            }
        }
    }

    public static void evaluateTree(Path src, Path dst, Map<String, Object> context) throws IOException {
        try (Stream<Path> tree = Files.walk(src)) {
            tree.forEach(p -> {
                try {
                    Path target = evaluatePath(dst.resolve(src.relativize(p)), context);
                    evaluate(p, target, context);
                } catch (IOException e) {
                    throw new RuntimeException(IOEXCEPTION_WRAPPER, e);
                }
            });
        } catch (RuntimeException e) {
            if (e.getMessage().equals(IOEXCEPTION_WRAPPER))
                throw (IOException) e.getCause();
            throw e;
        }
    }

    private static Path evaluatePath(Path path, Map<String, Object> properties) {
        String s = path.toString();
        return s.contains("{{") ? Paths.get(evaluate(s, properties)) : path;
    }
}
