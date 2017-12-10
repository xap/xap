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
package org.gigaspaces.cli;

import org.gigaspaces.cli.commands.VersionCommand;
import picocli.CommandLine;

import java.util.Arrays;
import java.util.Collection;

@CommandLine.Command(name="gs", header = {
        "@|green    _____           _       _     _   ______    _                 |@",
        "@|green   |_   _|         (_)     | |   | | |  ____|  | |                |@",
        "@|green     | |  _ __  ___ _  __ _| |__ | |_| |__   __| | __ _  ___      |@",
        "@|green     | | | '_ \\/ __| |/ _` | '_ \\| __|  __| / _` |/ _` |/ _ \\  |@",
        "@|green    _| |_| | | \\__ \\ | (_| | | | | |_| |___| (_| | (_| |  __/   |@",
        "@|green   |_____|_| |_|___/_|\\__, |_| |_|\\__|______\\__,_|\\__, |\\___||@",
        "@|green                       __/ |                       __/ |          |@",
        "@|green                      |___/                       |___/           |@",
})
public class XapMainCommand extends CliCommand {
    protected void execute() throws Exception {
    }

    public static void main(String[] args) {
        CliExecutor.main(args, new XapMainCommand(), getSubcommands());
    }

    public static Collection<?> getSubcommands() {
        return Arrays.asList(new VersionCommand());
    }
}
