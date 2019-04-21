package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliExecutor;

import java.io.IOException;

public class Autocomplete {

    public static void main(final String[] args) throws IOException {
        CliExecutor.generateAutoComplete(new XapMainCommand(), args);
    }
}
