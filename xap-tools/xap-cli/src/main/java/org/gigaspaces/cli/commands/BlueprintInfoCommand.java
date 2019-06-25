package org.gigaspaces.cli.commands;

import org.gigaspaces.blueprints.Blueprint;
import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.commands.utils.KeyValueFormatter;
import picocli.CommandLine;

@CommandLine.Command(name = "info", header = "Show information for the specified blueprint")
public class BlueprintInfoCommand extends CliCommand {

    @CommandLine.Parameters(index = "0", description = "Blueprint name", completionCandidates = BlueprintCommand.BlueprintCompletionCandidates.class)
    private String name;

    @Override
    protected void execute() throws Exception {
        Blueprint blueprint = BlueprintCommand.getBlueprint(name);

        System.out.println("Blueprint information for " + name + ":");
        System.out.println("--- Values ---");
        KeyValueFormatter formatter = KeyValueFormatter.builder().build();
        blueprint.getValues().forEach(formatter::append);
        System.out.println(formatter.get());
    }
}
