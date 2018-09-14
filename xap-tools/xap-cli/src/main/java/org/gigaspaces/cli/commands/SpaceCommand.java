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

import com.gigaspaces.start.SystemInfo;
import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.SubCommandContainer;
import picocli.CommandLine.*;

import java.util.ArrayList;
import java.util.Collection;

@Command(name="space", header = "List of available commands for Space operations")
public class SpaceCommand extends CliCommand implements SubCommandContainer {

    @Override
    protected void execute() throws Exception {
    }

    @Override
    public Collection<Object> getSubCommands() {
        Collection<Object> result = new ArrayList<Object>();
        // This command is not supported in XAP.NET
        if (SystemInfo.singleton().locations().xapNetHome() == null)
            result.add(new SpaceRunCommand());
        return result;
    }
}
