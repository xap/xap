package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliExecutor;
import picocli.CommandLine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Autocomplete {

    public static void main(final String[] args) throws IOException {

        CommandLine mainCommand = CliExecutor.toCommandLine(new XapMainCommand());

        String alias;
        if (args.length != 0) {
            alias = args[0];
        } else {
            alias = mainCommand.getCommandName();
        }

        File path = new File(alias + "-autocomplete");
        String generatedScript = picocli.AutoComplete.bash(alias, mainCommand);
        FileWriter scriptWriter = null;

        try{
            scriptWriter = new FileWriter(path.getPath());
            scriptWriter.write(generatedScript);
        } finally {
            if(scriptWriter != null){
                scriptWriter.close();
            }
        }
    }
}
