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
