package org.gigaspaces.cli;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        sortOptions = false,
        //headerHeading = "",
        header = "<header goes here>",
        synopsisHeading = "%nUsage: ",
        descriptionHeading = "%nDescription: ",
        //description = "<description goes here>",
        parameterListHeading = "%nParameters:%n",
        optionListHeading = "%nOptions:%n")
public abstract class CliCommand implements Callable<Object> {

    @CommandLine.Option(names = {"--help"}, usageHelp = true, description = "display this help message")
    boolean usageHelpRequested;

    @Override
    public Object call() throws Exception {
        beforeExecute();
        execute();
        return null;
    }

    protected void beforeExecute() {
    }

    protected abstract void execute() throws Exception;
}
