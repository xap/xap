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
import java.util.Collections;
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
    private Map<String, Object> properties;

    @Option(names = {"-i", "--interactive" }, description = "Set values on the command line (can specify multiple or separate values with commas: key1=val1,key2=val2)")
    private Boolean interactive;

    @Override
    protected void execute() throws Exception {
        Blueprint blueprint = BlueprintCommand.getBlueprint(this.name);
        Path target = assertNotExists(this.target);
        Map<String, Object> properties = this.properties != null ? this.properties : new HashMap<>();

        final LineReader interactiveReader = initInteractive(blueprint, properties);
        if (interactiveReader == null) {
            if (blueprint == null)
                throw CliCommandException.userError("Blueprint name was not provided and interactive mode is off");
            if (target == null) {
                target = getDefaultTarget(blueprint);
                System.out.println("Target not specified - auto-selected " + target);
            }
        } else {
            if (blueprint == null)
                blueprint = readBlueprint(interactiveReader);
            if (target == null)
                target = readTarget(interactiveReader, getDefaultTarget(blueprint));
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

    private LineReader initInteractive(Blueprint blueprint, Map<String, Object> properties) throws IOException {
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
        KeyValueFormatter formatter = KeyValueFormatter.builder().build();
        BlueprintRepository repository = BlueprintCommand.getDefaultRepository();
        repository.getBlueprints().forEach(b -> formatter.append("[" + counter.incrementAndGet() + "] " + b.getName(), b.getDescription()));
        System.out.print(formatter.get());
        int defaultBlueprint = repository.indexOf("client").orElse(0) + 1;
        String input = readString(interactiveReader,"Select a blueprint by name or number", defaultBlueprint);
        Optional<Integer> code = isEmpty(input) ? Optional.of(defaultBlueprint) : tryParse(input);
        return code.isPresent()
                ? BlueprintCommand.getBlueprint(code.get() - 1)
                : BlueprintCommand.getBlueprint(input);
    }

    private static Path readTarget(LineReader interactiveReader, Path defaultTarget) throws CliCommandException {
        String input = readString(interactiveReader, "Target path", defaultTarget);
        Path result = !isEmpty(input) ? Paths.get(input) : defaultTarget;
        return assertNotExists(result);
    }

    private static void readProperties(LineReader interactiveReader, Map<String, Object> commandProperties, Map<String, String> blueprintProperties) {
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
