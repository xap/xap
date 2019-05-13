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

    @Parameters(index = "0", description = "Blueprint name")
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
