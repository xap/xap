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

import com.gigaspaces.internal.jvm.JavaUtils;
import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliExecutor;
import org.gigaspaces.cli.CommandsSet;
import org.gigaspaces.cli.SubCommandContainer;
import picocli.CommandLine.*;

@Command(name="gs", headerHeading = XapMainCommand.HEADER, customSynopsis = "gs.{sh|bat} [global-options] command [options] [parameters]")
public class XapMainCommand extends CliCommand implements SubCommandContainer {
    public static final String HEADER =
                    "@|green   __   __          _____                                   |@%n" +
                    "@|green   \\ \\ / /    /\\   |  __ \\                               |@%n" +
                    "@|green    \\ V /    /  \\  | |__) |                                |@%n" +
                    "@|green     > <    / /\\ \\ |  ___/                                 |@%n" +
                    "@|green    / . \\  / ____ \\| |                                     |@%n" +
                    "@|green   /_/ \\_\\/_/    \\_\\_|                                   |@%n" +
                    "%n";

    private static boolean isXapNet = System.getProperty("com.gs.xapnet.home") != null;

    @Option(names = "--cli-version", description = "Use another CLI version (set '1' for legacy CLI). Overrides XAP_CLI_VERSION environment variable", paramLabel = "<n>", defaultValue = "2")
    protected int cliVersion;

    protected void execute() throws Exception {
    }

    public static void main(String[] args) {
        CliExecutor.execute(new XapMainCommand(), args);
    }

    @Override
    public CommandsSet getSubCommands() {
        CommandsSet commandsSet = new CommandsSet();
        commandsSet.add(new VersionCommand());
        commandsSet.add(new HelpCommand());
        commandsSet.add(new DemoCommand());
        // This command is not supported in XAP.NET
        if (!isXapNet())
            commandsSet.add(new BlueprintCommand());
        commandsSet.add(new ProcessingUnitCommand());
        commandsSet.add(new SpaceCommand());
        // This command is not supported in XAP.NET
        if (!isXapNet())
            commandsSet.add(new MavenCommand());
        if (!JavaUtils.isWindows())
            commandsSet.add(new CompletionCommand());
        return commandsSet;
    }

    public static boolean isXapNet() {
        return isXapNet;
    }
}
