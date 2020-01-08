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

import com.gigaspaces.start.SystemLocations;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BlueprintUtils {

    private static BlueprintRepository defaultRepository;
    public static BlueprintRepository getDefaultRepository() throws IOException {
        if (defaultRepository == null) {
            defaultRepository = new BlueprintRepository(SystemLocations.singleton().config("blueprints"));
        }
        return defaultRepository;
    }

    public static Blueprint getBlueprint(String name) throws IOException {
        if (name == null || name.length() == 0)
            return null;
        BlueprintRepository repository = getDefaultRepository();

        return repository.get(name);
    }

    public static Blueprint getBlueprint(int id) throws IOException {
        return getDefaultRepository().get(id);
    }

    public static Path getDefaultTarget(Blueprint blueprint){
        String name = "my-" + blueprint.getName();
        int suffix = 1;
        Path path;
        for (path = Paths.get(name) ; Files.exists(path); path = Paths.get(name + suffix++));
        return path;

    }
}
