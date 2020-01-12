package org.gigaspaces.blueprints;

import com.gigaspaces.start.SystemLocations;
import org.jini.rio.boot.PUZipUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.gigaspaces.blueprints.BlueprintUtils.getDefaultTarget;

public class ProcessingUnitGenerator {
    static Logger logger = Logger.getLogger(ProcessingUnitGenerator.class.getName());

    public static void generate(String name, Map<String, String> properties) throws Exception {
        Blueprint blueprint = BlueprintUtils.getBlueprint(name);

        Path workDir = SystemLocations.singleton().work();
        Path target = getDefaultTarget(blueprint , Files.createTempDirectory(workDir,"blueprint-temp"));

        logger.info("************************* Target Path: "+target.toString());
        logger.info("************************* workDir Path: "+workDir.toString());

        blueprint.generate(target, updatedProperties(blueprint.getValues(),properties));
        File zipFile = new File(target.toString());
        PUZipUtils.zip(target.toFile(), zipFile);

        logger.info("************************* zipFile exist: "+zipFile.exists());
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
