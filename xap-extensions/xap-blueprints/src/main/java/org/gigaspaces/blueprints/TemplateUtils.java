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

import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.jvm.JavaUtils;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * @author Niv Ingberg
 * @since 14.5
 */
public class TemplateUtils {
    private static final MustacheFactory mf = new DefaultMustacheFactory();

    public static String evaluate(String text, Object scope) {
        Mustache m = mf.compile(new StringReader(text), "temp.name");
        return m.execute(new StringWriter(), scope).toString();
    }

/*    public static String evaluateResource(String resourceName, Object scope) throws IOException {
        return evaluate(BootIOUtils.readAsString( BootIOUtils.getResourceAsStream(resourceName)), scope);
    }*/

    public static String evaluateResource(String resourceName, Object scope) throws IOException {
        return evaluate(BootIOUtils.readAsString( BootIOUtils.getResourceAsStream( resourceName ) ), scope );
    }

    public static void evaluateTree(Path src, Path dst, Object scope) throws IOException {
        try (Stream<Path> tree = Files.walk(src)) {
            tree.forEach(p -> {
                try {
                    Path target = evaluatePath(dst.resolve(src.relativize(p)), scope);
                    if (Files.isDirectory(p)) {
                        Files.createDirectories(target);
                    } else {
                        try (Reader reader = Files.newBufferedReader(p)) {
                            try (Writer writer = Files.newBufferedWriter(target)) {
                                mf.compile(reader, "temp.name").execute(writer, scope);
                            }
                        }
                        if(!JavaUtils.isWindows()) {
                            Files.setPosixFilePermissions(target, Files.getPosixFilePermissions(p));
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static Path evaluatePath(Path path, Object scope) {
        String s = path.toString();
        return s.contains("{{") ? Paths.get(evaluate(s, scope)) : path;
    }
}
