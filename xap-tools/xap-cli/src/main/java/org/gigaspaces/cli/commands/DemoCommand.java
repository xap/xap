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
import org.gigaspaces.cli.commands.utils.XapCliUtils;

import picocli.CommandLine.Command;

/**
 * @since 12.3
 * @author Rotem Herzberg
 */
@Command(name = "demo", header = "Run a Space in high availability mode (2 primaries with 1 backup each)")
public class DemoCommand extends CliCommand {

    @Override
    protected void execute() throws Exception {
        SpaceRunCommand command = new SpaceRunCommand();
        command.name = XapCliUtils.DEMO_SPACE_NAME;
        command.partitions = 2;
        command.ha = true;
        command.lus = true;
        command.execute();
    }
}
