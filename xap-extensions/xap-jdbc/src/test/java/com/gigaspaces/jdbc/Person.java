package com.gigaspaces.jdbc;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;

public class Person {

    private Integer id;
    private Integer organizationId;
    private String firstName;
    private String lastName;
    private Double salary;

    public Person(){

    }

    public Person(Integer id, Integer organizationId, String firstName, String lastName, Double salary) {
        this.id = id;
        this.organizationId = organizationId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.salary = salary;
    }

    @SpaceId
    public Integer getId() {
        return id;
    }
    public Person setId(Integer id) {
        this.id = id;
        return this;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public Integer getOrganizationId() {
        return organizationId;
    }
    public Person setOrganizationId(Integer organizationId) {
        this.organizationId = organizationId;
        return this;
    }

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

    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public Double getSalary() {
        return salary;
    }
    public Person setSalary(Double salary) {
        this.salary = salary;
        return this;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", organizationId=" + organizationId +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", salary=" + salary +
                '}';
    }
}
