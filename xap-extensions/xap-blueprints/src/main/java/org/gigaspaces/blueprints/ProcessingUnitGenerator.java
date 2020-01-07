package org.gigaspaces.blueprints;

import java.nio.file.Path;
import java.util.Map;

import static org.gigaspaces.blueprints.BlueprintUtils.getDefaultTarget;

public class ProcessingUnitGenerator {

    public static void generate(String name, Map<String, String> properties) throws Exception {
        Blueprint blueprint = BlueprintUtils.getBlueprint(name);

        Path target = getDefaultTarget(blueprint);

        blueprint.generate(target, updatedProperties(blueprint.getValues(),properties));
    }

    private static Map<String, String> updatedProperties(Map<String, String> defaultProperties, Map<String, String> properties) {

        defaultProperties.forEach((k, v) -> {
            if (!properties.containsKey(k)) {
                properties.put(k, defaultProperties.get(k));
            }
        });

        return properties;
    }
}
