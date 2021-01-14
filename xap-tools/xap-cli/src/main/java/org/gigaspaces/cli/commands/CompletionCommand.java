package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliExecutor;
import picocli.CommandLine;

import java.io.File;
import java.io.FileWriter;

@CommandLine.Command(name="completion", header = {
        "Generate completion script for bash/zsh shells.",
        "",
        "The generated script should be sourced to provide interactive completion.",
        "",
        "  (*) To load the gs completion code for bash into the current shell:",
        "    @|bold   $ source <(./gs.sh completion --stdout)|@",
        "",
        "      Alternatively, you can use the @|bold --target|@ option to create the",
        "      completion code in a file and then source it:",
        "    @|bold   $ ./gs.sh completion --target gs.completion|@",
        "    @|bold   $ source gs.completion|@",
        "",
        "  (*) To load the gs completion for each new shell: ",
        "      Source this script from the @|bold .bash_profile|@ to be loaded on each session.",
        "",
        "      You may also leverage bash completion behavior by placing the script",
        "      in the @|bold bash_completion.d|@ folder: ",
        "    @|bold   $ ./gs.sh completion --target <path to>/bash_completion.d/gs.completion|@",
        "",
        "For additional OS specific instructions and considerations refer to:",
        "https://docs.gigaspaces.com/16.0/admin/tools-cli.html#AutocompleteFunctionality",
        ""
})
public class CompletionCommand extends CliCommand {

    @CommandLine.Option(names = {"--target" }, description = "Stores the auto completion code in the specified path")
    private File target;

    @CommandLine.Option(names = {"--stdout" }, description = "Prints the auto completion code to standard output")
    private boolean stdout;

    @Override
    protected void execute() throws Exception {
        CommandLine mainCommand = CliExecutor.getMainCommand();
        String output = picocli.AutoComplete.bash(mainCommand.getCommandName(), mainCommand);

        if (target != null) {
            try (FileWriter scriptWriter = new FileWriter(target)) {
                scriptWriter.write(output);
            }
        }

        if (stdout) {
            System.out.println(output);
        }

        if (target == null && !stdout) {
            CliExecutor.getCurrentCommandLine().usage(System.out);
        }
    }
}
