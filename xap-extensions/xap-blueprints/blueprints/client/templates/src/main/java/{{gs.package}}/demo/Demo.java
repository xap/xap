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
package {{maven.groupId}}.demo;

import com.j_spaces.core.client.SQLQuery;
import org.openspaces.core.GigaSpace;

/**
 * A demo of common space operations
 */
public class Demo {
    public static void run(GigaSpace gigaSpace) {
        SQLQuery<Person> queryAll = new SQLQuery<>(Person.class, "");

        int count = gigaSpace.count(queryAll);
        System.out.println("Count persons in space: " + count);

        System.out.println("Clear all persons from space");
        gigaSpace.clear(queryAll);

        int entries = 10;
        System.out.println("Write " + entries + " entries to space...");
        for (int i=0 ; i < entries ; i++) {
            gigaSpace.write(new Person().setId(i).setFirstName("John").setLastName("Doe"));
        }

        count = gigaSpace.count(queryAll);
        System.out.println("Count all persons: " + count);

        SQLQuery<Person> querySome = new SQLQuery<>(Person.class, "id < ?", 5);
        count = gigaSpace.count(querySome);
        System.out.println("Count persons with id < 5: " + count);

        System.out.println("Iterating over those persons:");
        for (Person person : gigaSpace.iterator(querySome)) {
            System.out.println(person.toString());
        }
        System.out.println("Demo completed");
    }
}
