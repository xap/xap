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
