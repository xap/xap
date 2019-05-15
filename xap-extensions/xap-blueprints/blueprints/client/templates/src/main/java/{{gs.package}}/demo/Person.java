package {{maven.groupId}}.demo;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;

/**
 * A space class can be a simple POJO with getters and setters.
 * One property must be annotated with @SpaceId to define the primary key.
 * Additional properties may be indexed to boost query performance using @SpaceIndex
 * For more information on modeling space data see the documentation.
 */
public class Person {
    private int id;
    private String firstName;
    private String lastName;

    @Override
    public String toString() {
        return String.format("Person [id=%s, firstName=%s, lastName=%s]", id, firstName, lastName);
    }

    @SpaceId
    public int getId() {
        return id;
    }

    public Person setId(int id) {
        this.id = id;
        return this;
    }

    @SpaceIndex
    public String getFirstName() {
        return firstName;
    }

    public Person setFirstName(String firstName) {
        this.firstName = firstName;
        return this;
    }

    public String getLastName() {
        return lastName;
    }

    public Person setLastName(String lastName) {
        this.lastName = lastName;
        return this;
    }
}
