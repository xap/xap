package org.openspaces.test.persistency.hibernate;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Child {

    @Id
    private String name;

    Child() {
    }

    public Child(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }
}
