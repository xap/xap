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
package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliCommandException;
import org.gigaspaces.blueprints.Blueprint;
import picocli.CommandLine.*;

import java.awt.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@Command(name="generate", header = "Generates a new GigaSpaces project from the specified blueprint")
public class BlueprintGenerateCommand extends CliCommand {

    @Parameters(index = "0", description = "Blueprint name", completionCandidates = BlueprintCommand.BlueprintCompletionCandidates.class)
    private String name;

    @Parameters(index = "1", description = "Target path for generated project", arity = "0..1")
    private String target;

    @Option(names = {"--set" }, description = "Set values on the command line (can specify multiple or separate values with commas: key1=val1,key2=val2)", split = ",")
    private Map<String, Object> properties;

    @Override
    protected void execute() throws Exception {
        Blueprint blueprint = BlueprintCommand.getBlueprint(name);
        Path targetPath = getTargetPath(this.target);
        blueprint.generate(targetPath, properties);
        System.out.println(String.format("Generated project from %s at %s", name, targetPath.toAbsolutePath()));
        if (Desktop.isDesktopSupported())
            Desktop.getDesktop().open(targetPath.toFile());
    }

    private Path getImplicitTarget(String name) {
        int suffix = 1;
        Path path;
        for (path = Paths.get(name) ; Files.exists(path); path = Paths.get(name + suffix++));
        return path;
    }

    private Path getTargetPath(String target) throws CliCommandException {
        Path result;
        if (target == null || target.length() == 0) {
            result = getImplicitTarget("my-" + name);
            System.out.println("Target not specified - auto-selected " + result);
            return result;
        } else {
            result = Paths.get(target);
            if (Files.exists(result))
                throw new CliCommandException("Target path already exists: " + target);
            return result;
        }
    }
}
