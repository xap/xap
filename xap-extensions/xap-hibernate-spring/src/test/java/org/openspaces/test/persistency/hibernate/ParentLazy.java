package org.openspaces.test.persistency.hibernate;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Set;

@Entity
public class ParentLazy implements Parent {
    @Id
    private String name;
    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private Set<Child> children = new HashSet<>();

    public ParentLazy() {}
    public ParentLazy(String name, String ... children) {
        this.name = name;
        for (String child : children) {
            this.children.add(new Child(child));
        }
    }

    public String getName() {
        return name;
    }

    public Set<Child> getChildren() {
        return children;
    }
}
