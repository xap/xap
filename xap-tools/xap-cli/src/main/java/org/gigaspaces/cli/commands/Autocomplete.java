/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
