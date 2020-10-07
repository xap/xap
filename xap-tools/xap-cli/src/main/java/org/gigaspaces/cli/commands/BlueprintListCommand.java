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

import org.gigaspaces.blueprints.BlueprintRepository;
import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.commands.utils.KeyValueFormatter;
import picocli.CommandLine;

@CommandLine.Command(name = "list", header = "Lists available blueprints")
public class BlueprintListCommand extends CliCommand {
    @Override
    protected void execute() throws Exception {
        BlueprintRepository repository = BlueprintRepository.getDefault();
        System.out.println("Available blueprints:");
        System.out.println("---------------------");
        KeyValueFormatter formatter = KeyValueFormatter.builder().build();
        repository.getBlueprints().forEach(b -> formatter.append(b.getName(), b.getDescription()));
        System.out.print(formatter.get());
    }
}
