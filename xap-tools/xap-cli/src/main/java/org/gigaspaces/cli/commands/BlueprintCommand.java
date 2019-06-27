package org.gigaspaces.cli.commands;

import com.gigaspaces.logger.LoggerSystemInfo;
import org.gigaspaces.blueprints.Blueprint;
import org.gigaspaces.blueprints.BlueprintRepository;
import org.gigaspaces.cli.*;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

@CommandLine.Command(name="blueprint", header = "List of available commands for blueprints")
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

    private static BlueprintRepository defaultRepository;
    static BlueprintRepository getDefaultRepository() throws IOException {
        if (defaultRepository == null) {
            defaultRepository = new BlueprintRepository(Paths.get(LoggerSystemInfo.xapHome, "config", "blueprints"));
        }
        return defaultRepository;
    }

    static Blueprint getBlueprint(String name) throws IOException, CliCommandException {
        if (name == null || name.length() == 0)
            return null;
        BlueprintRepository repository = getDefaultRepository();
        Blueprint blueprint = repository.get(name);
        if (blueprint == null)
            throw CliCommandException.userError("Unknown blueprint: " + name + ". Available blueprints: " + repository.getNames());
        return blueprint;
    }

    static Blueprint getBlueprint(int id) throws IOException {
        return getDefaultRepository().get(id);
    }

    public static class BlueprintCompletionCandidates extends ArrayList<String> {
        private static final Collection<String> names = getNames();

        public BlueprintCompletionCandidates() {
            super(names);
        }

        private static Collection<String> getNames() {
            try {
                return getDefaultRepository().getNames();
            } catch (Exception e) {
                System.out.println("Warning: failed to get blueprints for autocomplete - " + e.getMessage());
                return Collections.emptyList();
            }
        }
    }
}
