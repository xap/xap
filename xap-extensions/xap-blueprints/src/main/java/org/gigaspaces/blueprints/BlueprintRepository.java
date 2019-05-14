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
