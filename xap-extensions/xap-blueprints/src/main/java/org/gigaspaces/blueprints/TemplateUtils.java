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
import java.util.Map;
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

    public static String evaluateResource(String resourceName, Object scope) throws IOException {
        return evaluate(BootIOUtils.readAsString(BootIOUtils.getResourcePath(resourceName)), scope);
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
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static Path evaluatePath(Path path, Map<String, Object> properties) {
        String s = path.toString();
        return s.contains("{{") ? Paths.get(evaluate(s, properties)) : path;
    }
}
