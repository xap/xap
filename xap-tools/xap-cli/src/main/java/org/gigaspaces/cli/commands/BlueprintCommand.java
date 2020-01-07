package org.gigaspaces.cli.commands;

import com.gigaspaces.start.SystemLocations;
import org.gigaspaces.blueprints.Blueprint;
import org.gigaspaces.blueprints.BlueprintRepository;
import org.gigaspaces.blueprints.BlueprintUtils;
import org.gigaspaces.cli.*;
import picocli.CommandLine;

import java.io.IOException;
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

    static Blueprint getBlueprint(String name) throws IOException, CliCommandException {
        Blueprint blueprint = BlueprintUtils.getBlueprint(name);
        if (blueprint == null)
            throw CliCommandException.userError("Unknown blueprint: " + name + ". Available blueprints: " + BlueprintUtils.getDefaultRepository().getNames());
        return blueprint;
    }

    static Blueprint getBlueprint(int id) throws IOException {
        return BlueprintUtils.getBlueprint(id);
    }

    public static class BlueprintCompletionCandidates extends ArrayList<String> {
        private static final Collection<String> names = getNames();

        public BlueprintCompletionCandidates() {
            super(names);
        }

        private static Collection<String> getNames() {
            try {
                return BlueprintUtils.getDefaultRepository().getNames();
            } catch (Exception e) {
                System.out.println("Warning: failed to get blueprints for autocomplete - " + e.getMessage());
                return Collections.emptyList();
            }
        }
    }
}
