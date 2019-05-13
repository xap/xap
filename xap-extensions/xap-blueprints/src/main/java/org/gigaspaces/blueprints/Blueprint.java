package org.gigaspaces.blueprints;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Niv Ingberg
 * @since 14.5
 */
public class Blueprint {
    private static final String TEMPLATES_PATH = "templates";
    private static final String INFO_PATH = "blueprint.txt";
    private static final String VALUES_PATH = "values.txt";
    private static final Map<String, Object> defaultContext = initDefaultContext();

    private final String name;
    private final Path content;
    private final Path valuesPath;
    private final Map<String, String> properties;

    public Blueprint(Path home) {
        this.name = home.getFileName().toString();
        this.content = home.resolve(TEMPLATES_PATH);
        this.valuesPath = home.resolve(VALUES_PATH);
        try {
            this.properties = load(home.resolve(INFO_PATH));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load blueprint information" , e);
        }
    }

    public static boolean isValid(Path path) {
        return Files.exists(path) &&
                Files.exists(path.resolve(TEMPLATES_PATH)) &&
                Files.exists(path.resolve(INFO_PATH)) &&
                Files.exists(path.resolve(VALUES_PATH));
    }

    public static Collection<Blueprint> fromPath(Path path) throws IOException {
        return Files.list(path)
                .filter(Blueprint::isValid)
                .map(Blueprint::new)
                .collect(Collectors.toList());
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return getInfo("description");
    }

    public String getInfo(String key) {
        return properties.get(key);
    }

    public Map<String, String> getValues() throws IOException {
        return load(valuesPath);
    }

    public void generate(Path target) throws IOException {
        generate(target, Collections.emptyMap());
    }

    public void generate(Path target, Map<String, Object> userOverrides) throws IOException {
        if (Files.exists(target))
            throw new IllegalArgumentException("Target already exists: " + target);

        Map<String, Object> context = loadContext(userOverrides);
        TemplateUtils.evaluateTree(content, target, context);
    }

    private Map<String, Object> loadContext(Map<String, Object> userOverrides) throws IOException {
        Map<String, Object> result = new HashMap<>(defaultContext);
        result.putAll(getValues());
        if (userOverrides != null)
            result.putAll(userOverrides);
        return expand(result);
    }

    private Map<String, Object> expand(Map<String, Object> context) {
        boolean replaced = false;
        for (Map.Entry<String, Object> entry : context.entrySet()) {
            Optional<Object> newValue = expand(entry.getValue(), context);
            if (newValue.isPresent()) {
                entry.setValue(newValue.get());
                replaced = true;
            }
        }
        // TODO: if replaced expand again.
        return context;
    }

    private static Optional<Object> expand(Object value, Map<String, Object> context) {
        if (!(value instanceof String))
            return Optional.empty();
        String s = (String) value;
        if (!s.contains("{{"))
            return Optional.empty();
        return Optional.of(TemplateUtils.evaluate(s, context));
    }

    private static Map<String, Object> initDefaultContext() {
        Map<String, Object> result = new HashMap<>();
        result.put("gs.path", (Function<String, String>) s -> s.replace('.', File.separatorChar));
        return result;
    }

    private static Map<String, String> load(Path path) throws IOException {
        Map<String, String> result = new HashMap<>();
        try (FileInputStream stream = new FileInputStream(path.toString())) {
            Properties p = new Properties();
            p.load(stream);
            p.forEach((k, v) -> result.put(k.toString(), v.toString()));
        }
        return result;
    }
}
