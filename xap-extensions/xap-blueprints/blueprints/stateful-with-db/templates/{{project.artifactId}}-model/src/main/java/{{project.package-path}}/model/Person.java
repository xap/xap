package {{project.groupId}}.model;

import com.gigaspaces.annotation.pojo.*;
import javax.persistence.*;

@Entity
@Table(name = "Person")
@SpaceClass
public class Person {
    private Integer id;
    private String lastName;
    private String firstName;
    private Integer age;

    public Person() {
    }

    @Id
    @SpaceId
    @SpaceIndex(unique = true)
    @Column(name = "Id")
    public Integer getId() {
        return id;
    }
    public void setId(Integer id) {
        this.id = id;
    }

    @SpaceIndex
    @Column(name = "LastName")
    public String getLastName() {
        return lastName;
    }
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Column(name = "FirstName")
    public String getFirstName() {
        return firstName;
    }
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    @Column(name = "Age")
    public Integer getAge() {
        return age;
    }
    public void setAge(Integer age) {
        this.age = age;
    }
}
