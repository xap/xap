package org.gigaspaces.blueprints;

import com.gigaspaces.start.SystemLocations;
import org.jini.rio.boot.PUZipUtils;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.logging.Logger;

public class ProcessingUnitGenerator {

    private static Logger logger = Logger.getLogger(ProcessingUnitGenerator.class.getName());

    public static File generate(String name, Map<String, String> properties) throws Exception {
        Blueprint blueprint = BlueprintRepository.getDefault().get(name);

        Path workDir = SystemLocations.singleton().work();
        Path tempDirectory = Files.createTempDirectory(workDir, "blueprint-temp");
        Path blueprintDir = Files.createDirectory(tempDirectory.resolve("blueprint"));
        Path target = blueprintDir.resolve(name);

        blueprint.generate(target, properties);
        Path zipFile = blueprintDir.resolve(target.getFileName()+".zip");

        PUZipUtils.zip(blueprintDir.toFile(), zipFile.toFile());
        logger.info("************************* Target Path: "+target.toString());

        return zipFile.toFile();
    }
}
