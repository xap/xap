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

import org.gigaspaces.blueprints.Blueprint;
import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.commands.utils.KeyValueFormatter;
import picocli.CommandLine;

@CommandLine.Command(name = "info", header = "Show information for the specified blueprint")
public class BlueprintInfoCommand extends CliCommand {

    @CommandLine.Parameters(index = "0", description = "Blueprint name", completionCandidates = BlueprintCommand.BlueprintCompletionCandidates.class)
    private String name;

    @Override
    protected void execute() throws Exception {
        Blueprint blueprint = BlueprintCommand.getBlueprint(name);

        System.out.println("Blueprint information for " + name + ":");
        System.out.println("--- Values ---");
        KeyValueFormatter formatter = KeyValueFormatter.builder().build();
        blueprint.getValues().forEach(formatter::append);
        System.out.println(formatter.get());
    }
}
