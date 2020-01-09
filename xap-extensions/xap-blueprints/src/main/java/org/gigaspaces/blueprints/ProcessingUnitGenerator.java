package org.gigaspaces.blueprints;

import java.util.Map;

public class ProcessingUnitGenerator {

    public static void generate(String name, Map<String, String> properties) throws Exception {
        Blueprint blueprint = BlueprintRepository.getDefault().get(name);
        blueprint.generate(blueprint.getDefaultTarget(), properties);
    }
}
