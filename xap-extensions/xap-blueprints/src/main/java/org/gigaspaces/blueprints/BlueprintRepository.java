package org.gigaspaces.blueprints;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * @author Niv Ingberg
 * @since 14.5
 */
public class BlueprintRepository {

    private final Map<String, Blueprint> blueprints;

    public BlueprintRepository(Path home) throws IOException {
        Objects.requireNonNull(home);
        if (!Files.exists(home))
            throw new IllegalArgumentException("Template manager home does not exist: " + home);
        this.blueprints = new HashMap<>();
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
}
