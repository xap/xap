package org.gigaspaces.cli.commands;

import org.gigaspaces.blueprints.Blueprint;
import org.gigaspaces.blueprints.BlueprintRepository;
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
        if (name == null || name.length() == 0)
            return null;
        Blueprint blueprint = BlueprintRepository.getDefault().get(name);
        if (blueprint == null)
            throw CliCommandException.userError("Unknown blueprint: " + name + ". Available blueprints: " + BlueprintRepository.getDefault().getNames());
        return blueprint;
    }

    public static class BlueprintCompletionCandidates extends ArrayList<String> {
        static final long serialVersionUID = -8864509507204226171L;
        private static final Collection<String> names = getNames();

        public BlueprintCompletionCandidates() {
            super(names);
        }

        private static Collection<String> getNames() {
            try {
                return BlueprintRepository.getDefault().getNames();
            } catch (Exception e) {
                System.out.println("Warning: failed to get blueprints for autocomplete - " + e.getMessage());
                return Collections.emptyList();
            }
        }
    }
}
