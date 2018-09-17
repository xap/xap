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

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliExecutor;
import org.gigaspaces.cli.SubCommandContainer;
import picocli.CommandLine.*;

import java.util.Arrays;
import java.util.Collection;

@Command(name="xap", headerHeading = XapMainCommand.HEADER, customSynopsis = "xap [global-options] command [options] [parameters]")
public class XapMainCommand extends CliCommand implements SubCommandContainer {
    public static final String HEADER =
                    "@|green   __   __          _____                                   |@%n" +
                    "@|green   \\ \\ / /    /\\   |  __ \\                               |@%n" +
                    "@|green    \\ V /    /  \\  | |__) |                                |@%n" +
                    "@|green     > <    / /\\ \\ |  ___/                                 |@%n" +
                    "@|green    / . \\  / ____ \\| |                                     |@%n" +
                    "@|green   /_/ \\_\\/_/    \\_\\_|                                   |@%n" +
                    "%n";

    protected void execute() throws Exception {
    }

    public static void main(String[] args) {
        CliExecutor.execute(new XapMainCommand(), args);
    }

    @Override
    public Collection<Object> getSubCommands() {
        return Arrays.asList(
                (Object)
                new VersionCommand(),
                new DemoCommand(),
                new ProcessingUnitCommand(),
                new SpaceCommand());
    }
}
