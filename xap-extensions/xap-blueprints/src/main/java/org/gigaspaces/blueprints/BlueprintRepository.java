package org.gigaspaces.blueprints;

import com.gigaspaces.start.SystemLocations;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * @author Niv Ingberg
 * @since 14.5
 */
public class BlueprintRepository {

    private static BlueprintRepository defaultRepository;

    public static BlueprintRepository getDefault() throws IOException {
        if (defaultRepository == null) {
            defaultRepository = new BlueprintRepository(SystemLocations.singleton().config("blueprints"));
        }
        return defaultRepository;
    }

    private final Map<String, Blueprint> blueprints;

    public BlueprintRepository(Path home) throws IOException {
        Objects.requireNonNull(home);
        if (!Files.exists(home))
            throw new IllegalArgumentException("Template manager home does not exist: " + home);
        this.blueprints = new LinkedHashMap<>();
        for (Blueprint blueprint : Blueprint.fromPath(home)) {
            blueprints.put(blueprint.getName(), blueprint);
        }
    }

    public Set<String> getNames() {
        return blueprints.keySet();
    }

    public Collection<Blueprint> getBlueprints() {
        return blueprints.values();
    }

    public Blueprint get(String name) {
        return blueprints.get(name);
    }

    public Blueprint get(int index) {
        for (Blueprint blueprint : blueprints.values()) {
            if (index-- == 0)
                return blueprint;
        }
        throw new IllegalStateException();
    }

    public Optional<Integer> indexOf(String name) {
        int index = 0;
        for (Map.Entry<String, Blueprint> entry : blueprints.entrySet()) {
            if (entry.getKey().equals(name))
                return Optional.of(index);
            index++;
        }
        return Optional.empty();
    }
}
