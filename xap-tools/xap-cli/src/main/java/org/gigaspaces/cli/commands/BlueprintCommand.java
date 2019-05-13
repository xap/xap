package org.gigaspaces.cli.commands;

import com.gigaspaces.start.SystemInfo;
import org.gigaspaces.blueprints.Blueprint;
import org.gigaspaces.blueprints.BlueprintRepository;
import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliCommandException;
import org.gigaspaces.cli.CommandsSet;
import org.gigaspaces.cli.SubCommandContainer;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Paths;

@CommandLine.Command(name="blueprint", aliases = {"bp"}, header = "List of available commands for blueprints")
public class BlueprintCommand extends CliCommand implements SubCommandContainer {

    @Override
    protected void execute() throws Exception {
    }

    @Override
    public CommandsSet getSubCommands() {
        return new CommandsSet()
                .add(new BlueprintListCommand())
                .add(new BlueprintInfoCommand())
                .add(new BlueprintGenerateCommand());
    }

    static BlueprintRepository getDefaultRepository() throws IOException {
        return new BlueprintRepository(Paths.get(SystemInfo.singleton().locations().config(), "blueprints"));
    }

    static Blueprint getBlueprint(String name) throws IOException, CliCommandException {
        BlueprintRepository repository = getDefaultRepository();
        Blueprint blueprint = repository.get(name);
        if (blueprint == null)
            throw new CliCommandException("Unknown blueprint: " + name + ". Available blueprints: " + repository.getNames());
        return blueprint;
    }
}
