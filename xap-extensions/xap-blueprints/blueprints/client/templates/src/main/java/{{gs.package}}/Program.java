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
package {{maven.groupId}};

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.CannotFindSpaceException;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.openspaces.core.space.SpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import {{maven.groupId}}.demo.Demo;

public class Program {
    public static void main(String[] args) {
        SpaceConfigurer spaceConfigurer;
        if (args.length == 0) {
            System.out.println("Space name not provided - creating an embedded space...");
            spaceConfigurer = new EmbeddedSpaceConfigurer("mySpace");
        } else {
            String spaceName = args[0];
            System.out.printf("Connecting to space %s...%n", spaceName);
            spaceConfigurer = new SpaceProxyConfigurer(spaceName);
        }

        try {
            GigaSpace gigaSpace = new GigaSpaceConfigurer(spaceConfigurer).create();
            System.out.println("Connected to space, running demo...");
            Demo.run(gigaSpace);
        } catch (CannotFindSpaceException e) {
            System.err.println("Failed to find space: " + e.getMessage());
        }

        spaceConfigurer.close();
        System.out.println("Program completed successfully");
        System.exit(0);
    }
}
