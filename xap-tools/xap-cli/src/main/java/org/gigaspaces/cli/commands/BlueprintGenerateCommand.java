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

import org.gigaspaces.blueprints.BlueprintRepository;
import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliCommandException;
import org.gigaspaces.blueprints.Blueprint;
import org.gigaspaces.cli.CliExecutor;
import org.gigaspaces.cli.commands.utils.KeyValueFormatter;
import org.jline.reader.LineReader;
import picocli.CommandLine.*;

import java.awt.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Command(name="generate", header = "Generate a new GigaSpaces project from the specified blueprint")
public class BlueprintGenerateCommand extends CliCommand {

    @Parameters(index = "0", description = "Blueprint name", arity = "0..1", completionCandidates = BlueprintCommand.BlueprintCompletionCandidates.class)
    private String name;

    @Parameters(index = "1", description = "Target path for generated project", arity = "0..1")
    private Path target;

    @Option(names = {"--set" }, description = "Set values on the command line (can specify multiple or separate values with commas: key1=val1,key2=val2)", split = ",")
    private Map<String, String> properties;

    @Option(names = {"-i", "--interactive" }, description = "Set values on the command line (can specify multiple or separate values with commas: key1=val1,key2=val2)")
    private Boolean interactive;

    @Override
    protected void execute() throws Exception {
        Blueprint blueprint = BlueprintCommand.getBlueprint(this.name);
        Path target = assertNotExists(this.target);
        Map<String, String> properties = this.properties != null ? this.properties : new HashMap<>();

        final LineReader interactiveReader = initInteractive(blueprint, properties);
        if (interactiveReader == null) {
            if (blueprint == null)
                throw CliCommandException.userError("Blueprint name was not provided and interactive mode is off");
            if (target == null) {
                target = blueprint.getDefaultTarget();
                System.out.println("Target not specified - auto-selected " + target);
            }
        } else {
            if (blueprint == null)
                blueprint = readBlueprint(interactiveReader);
            if (target == null)
                target = readTarget(interactiveReader, blueprint.getDefaultTarget());
            readProperties(interactiveReader, properties, blueprint.getValues());
        }

        blueprint.generate(target, properties);

        System.out.println(CliExecutor.formatAnsi(String.format(
                "@|bold,fg(green) Generated project from %s at %s|@", blueprint.getName(), target.toAbsolutePath())));
        if (Desktop.isDesktopSupported() &&
                interactiveReader != null &&
                readBoolean(interactiveReader, "Would you like to open it in file explorer?", true)) {
            Desktop.getDesktop().open(target.toFile());
        }
    }

    private LineReader initInteractive(Blueprint blueprint, Map<String, String> properties) throws IOException {
        String interactiveCause;
        if (interactive != null) {
            interactiveCause = this.interactive ? "interactive option was enabled" : null;
        } else if (blueprint == null) {
            interactiveCause = "blueprint name was not provided";
        } else if (properties.size() == 0) {
            interactiveCause = "no properties were provided";
        } else {
            interactiveCause = null;
        }
        LineReader result = null;
        if (interactiveCause != null) {
            System.out.println("Generating blueprint in interactive mode (" + interactiveCause + ")...");
            result = CliExecutor.getOrCreateReader();
            System.out.println("Please provide a value for each of the following, or click ENTER to accept the [default value]");
        }
        return result;
    }

    private static Blueprint readBlueprint(LineReader interactiveReader) throws CliCommandException, IOException {
        System.out.println("List of available blueprints: ");
        AtomicInteger counter = new AtomicInteger();
        BlueprintRepository repository = BlueprintRepository.getDefault();
        AtomicInteger maxLength = new AtomicInteger();
        repository.getBlueprints().forEach(b -> maxLength.set(Math.max(maxLength.get(), b.getName().length())));
        KeyValueFormatter formatter = KeyValueFormatter.builder().width(maxLength.get() + 6).build();
        repository.getBlueprints().forEach(b -> formatter.append("[" + counter.incrementAndGet() + "] " + b.getName(), b.getDescription()));
        System.out.print(formatter.get());
        int defaultBlueprint = repository.indexOf("client").orElse(0) + 1;
        String input = readString(interactiveReader,"Select a blueprint by name or number", defaultBlueprint);
        Optional<Integer> code = isEmpty(input) ? Optional.of(defaultBlueprint) : tryParse(input);
        return code.isPresent()
                ? BlueprintRepository.getDefault().get(code.get() - 1)
                : BlueprintCommand.getBlueprint(input);
    }

    private static Path readTarget(LineReader interactiveReader, Path defaultTarget) throws CliCommandException {
        String input = readString(interactiveReader, "Target path", defaultTarget);
        Path result = !isEmpty(input) ? Paths.get(input) : defaultTarget;
        return assertNotExists(result);
    }

    private static void readProperties(LineReader interactiveReader, Map<String, String> commandProperties, Map<String, String> blueprintProperties) {
        long missing = blueprintProperties.keySet().stream().filter(k -> !commandProperties.containsKey(k)).count();
        if (missing > 0 && readBoolean(interactiveReader,
                "There are " + missing + " additional properties in this blueprint - would you like to set them?", false)) {
            AtomicInteger curr = new AtomicInteger();
            blueprintProperties.forEach((k, v) -> {
                if (!commandProperties.containsKey(k)) {
                    String input = readString(interactiveReader, String.format("  (%s/%s) %s", curr.incrementAndGet(), missing, k), v);
                    commandProperties.put(k, input.isEmpty() ? v : input);
                }
            });
        }
    }

    private static String readString(LineReader reader, String label, Object defaultValue) {
        return reader.readLine(CliExecutor.formatAnsi(String.format(
                "%s [@|bold %s|@]: ", label, defaultValue)));
    }

    private static boolean readBoolean(LineReader lineReader, String label, boolean defaultValue) {
        if (lineReader == null)
            return defaultValue;
        String input = readString(lineReader, label, defaultValue ? "y" : "n");
        if (isEmpty(input))
            return defaultValue;
        switch (input.toLowerCase()) {
            case "y":
            case "yes":
                return true;
            default:
                return false;
        }
    }

    private static Path assertNotExists(Path path) throws CliCommandException {
        if (path != null && Files.exists(path))
            throw CliCommandException.userError("Target path already exists: " + path);
        return path;
    }

    private static boolean isEmpty(String s) {
        return s == null || s.length() == 0;
    }

    private static Optional<Integer> tryParse(String s) {
        try {
            return Optional.of(Integer.parseInt(s));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
