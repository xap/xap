package org.gigaspaces.blueprints;

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
