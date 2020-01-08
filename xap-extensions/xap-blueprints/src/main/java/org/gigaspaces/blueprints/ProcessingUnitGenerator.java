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
        Path target = Files.createTempDirectory(workDir,getDefaultTarget(blueprint).toString());

        logger.info("************************* Target Path: "+target.toString());

        blueprint.generate(target, updatedProperties(blueprint.getValues(),properties));
        File zipFile = new File(workDir.toString() + "zipFile.zip");
        PUZipUtils.zip(target.toFile(), zipFile);
        Path zipFilePath = Files.createFile(Paths.get(workDir.toString() + "zipFile2.zip"));
        pack(target, zipFilePath);
        logger.info("************************* zipFile exist: "+zipFile.exists());
        logger.info("************************* zipFile2 exist: "+zipFilePath.toFile().exists());

    }

    private static Map<String, String> updatedProperties(Map<String, String> defaultProperties, Map<String, String> properties) {

        defaultProperties.forEach((k, v) -> {
            if (!properties.containsKey(k)) {
                properties.put(k, defaultProperties.get(k));
            }
        });

        return properties;
    }

    public static void pack(Path sourceDirPath, Path zipFilePath) throws IOException {

        try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(zipFilePath))) {

            Files.walk(sourceDirPath)
                    .filter(path -> !Files.isDirectory(path))
                    .forEach(path -> {
                        ZipEntry zipEntry = new ZipEntry(sourceDirPath.relativize(path).toString());
                        try {
                            zs.putNextEntry(zipEntry);
                            Files.copy(path, zs);
                            zs.closeEntry();
                        } catch (IOException e) {
                            System.err.println(e);
                        }
                    });
        }
    }
}
