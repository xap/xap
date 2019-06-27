package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliExecutor;
import picocli.CommandLine;

import java.io.File;
import java.io.FileWriter;

@CommandLine.Command(name="completion", header = {
        "Generate completion script for bash/zsh shells.",
        "",
        "The generated script must be evaluated to provide interactive completion of GigaSpaces CLI commands.",
        "",
        "For additional OS specific instructions and considerations refer to:",
        "https://docs.gigaspaces.com/14.5/admin/tools-cli.html#AutocompleteFunctionality",
        "",
        "Examples:",
        "  1. Load the gs completion code for bash into the current shell:",
        "   @|bold   $ source <(./gs.sh completion --stdout)|@",
        "",
        "  2. Alternatively, you can use the @|bold --target|@ option to create the autocompletion",
        "     code in a file and then source it:",
        "   @|bold   $ ./gs.sh completion --target gs.completion|@",
        "   @|bold   $ source gs.completion|@",
        "",
        "     Source this script from the .bash_profile to auto-load on each session.",
        "",
        "  3. For completion to work automatically, bash shell completion behavior must be installed/supported.",
        "     Copy the generated completion code to the bash_completion.d folder.",
        "     All completion scripts under bash_completion.d folder are automatically sourced.",
        "   @|bold   $ ./gs.sh completion --target <path to>/bash_completion.d/gs.completion|@",
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
