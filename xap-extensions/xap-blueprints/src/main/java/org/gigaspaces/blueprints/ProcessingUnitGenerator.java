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
        Path zipFile = tempDirectory.resolve(target.getFileName()+".zip");

        PUZipUtils.zip(blueprintDir.toFile(), zipFile.toFile());
        logger.info("Target Path: "+target.toString());

        return zipFile.toFile();
    }
}
