package org.gigaspaces.cli.commands;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Command(name="generate", header = "Generates a new GigaSpaces project from the specified blueprint")
public class BlueprintGenerateCommand extends CliCommand {

    @Parameters(index = "0", description = "Blueprint name", arity = "0..1", completionCandidates = BlueprintCommand.BlueprintCompletionCandidates.class)
    private String name;

    @Parameters(index = "1", description = "Target path for generated project", arity = "0..1")
    private Path target;

    @Option(names = {"--set" }, description = "Set values on the command line (can specify multiple or separate values with commas: key1=val1,key2=val2)", split = ",")
    private Map<String, Object> properties;

    @Option(names = {"-i", "--interactive" }, description = "Set values on the command line (can specify multiple or separate values with commas: key1=val1,key2=val2)")
    private Boolean interactive;

    @Override
    protected void execute() throws Exception {
        this.properties = properties != null ? properties : Collections.emptyMap();

        assertNotExists(this.target);
        final LineReader interactiveReader = initInteractive();
        final Blueprint blueprint = initBlueprint(interactiveReader, this.name);
        final Path target = initTarget(interactiveReader, blueprint, this.target);
        final Map<String, Object> properties = initProperties(interactiveReader, blueprint, this.properties);

        blueprint.generate(target, properties);

        System.out.println(String.format("Generated project from %s at %s", blueprint.getName(), target.toAbsolutePath()));
        openResult(target, interactiveReader);
    }

    private void openResult(Path target, LineReader interactiveReader) throws IOException {
        if (Desktop.isDesktopSupported() && booleanQuestion(interactiveReader,
                "Would you like to open it using the file explorer?", false)) {
            Desktop.getDesktop().open(target.toFile());
        }
    }

    private LineReader initInteractive() throws IOException {
        String interactiveCause;
        if (interactive != null) {
            interactiveCause = this.interactive ? "interactive option was enabled" : null;
        } else if (isEmpty(name)) {
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
            System.out.println("Please provide a value for each of the following, or simply click ENTER to accept the default value in brackets.");
        }
        return result;
    }

    private static Blueprint initBlueprint(LineReader interactiveReader, String name) throws CliCommandException, IOException {
        if (!isEmpty(name))
            return BlueprintCommand.getBlueprint(name);

        if (interactiveReader == null)
            throw CliCommandException.userError("Blueprint name was not provided and interactive mode was disabled");

        System.out.println("List of available blueprints: ");
        AtomicInteger counter = new AtomicInteger();
        KeyValueFormatter formatter = KeyValueFormatter.builder().build();
        BlueprintCommand.getDefaultRepository().getBlueprints().forEach(b -> formatter.append("[" + counter.incrementAndGet() + "] " + b.getName(), b.getDescription()));
        System.out.print(formatter.get());
        int defaultBlueprint = BlueprintCommand.getDefaultRepository().indexOf("client").orElse(0) + 1;
        String input = interactiveReader.readLine(formatRequest("Select a blueprint by name or number", defaultBlueprint));
        Optional<Integer> code = isEmpty(input) ? Optional.of(defaultBlueprint) : tryParse(input);
        return code.isPresent()
                ? BlueprintCommand.getBlueprint(code.get() - 1)
                : BlueprintCommand.getBlueprint(input);
    }

    private static Path initTarget(LineReader interactiveReader, Blueprint blueprint, Path target) throws CliCommandException {
        if (target != null)
            return assertNotExists(target);

        Path defaultTarget = getDefaultTarget(blueprint);
        if (interactiveReader == null) {
            System.out.println("Target not specified - auto-selected " + defaultTarget);
            return defaultTarget;
        }

        String input = interactiveReader.readLine(formatRequest("Target path", defaultTarget));
        Path result = !isEmpty(input) ? Paths.get(input) : defaultTarget;
        return assertNotExists(result);
    }

    private static Map<String, Object> initProperties(LineReader interactiveReader, Blueprint blueprint, Map<String, Object> commandProperties)
            throws IOException {
        if (interactiveReader == null)
            return commandProperties;

        Map<String, String> blueprintProperties = blueprint.getValues();
        Map<String, Object> properties = new HashMap<>(commandProperties);
        long missing = blueprintProperties.keySet().stream().filter(k -> !properties.containsKey(k)).count();
        if (missing > 0 && booleanQuestion(interactiveReader,
                "There are " + missing + " additional properties in this blueprint - would you like to set them?", false)) {
            AtomicInteger curr = new AtomicInteger();
            blueprintProperties.forEach((k, v) -> {
                if (!properties.containsKey(k)) {
                    String input = interactiveReader.readLine(formatRequest(String.format("  (%s/%s) %s", curr.incrementAndGet(), missing, k), v));
                    properties.put(k, input.isEmpty() ? v : input);
                }
            });
        }
        return properties;
    }

    private static String formatRequest(String name, Object defaultValue) {
        return String.format("%s [%s]: ", name, defaultValue);
    }

    private static boolean booleanQuestion(LineReader lineReader, String question, boolean defaultValue) {
        if (lineReader == null)
            return defaultValue;
        String input = lineReader.readLine(formatRequest(question, defaultValue ? "y" : "n"));
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
    private static Path getDefaultTarget(Blueprint blueprint){
        String name = "my-" + blueprint.getName();
        int suffix = 1;
        Path path;
        for (path = Paths.get(name) ; Files.exists(path); path = Paths.get(name + suffix++));
        return path;

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
