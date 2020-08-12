package org.gigaspaces.cli.commands;

import org.gigaspaces.blueprints.BlueprintRepository;
import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.commands.utils.KeyValueFormatter;
import picocli.CommandLine;

@CommandLine.Command(name = "list", header = "Lists available blueprints")
public class BlueprintListCommand extends CliCommand {
    @Override
    protected void execute() throws Exception {
        BlueprintRepository repository = BlueprintRepository.getDefault();
        System.out.println("Available blueprints:");
        System.out.println("---------------------");
        KeyValueFormatter formatter = KeyValueFormatter.builder().build();
        repository.getBlueprints().forEach(b -> formatter.append(b.getName(), b.getDescription()));
        System.out.print(formatter.get());
    }
}
