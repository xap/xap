package org.openspaces.test.persistency.hibernate;

import java.util.Set;

public interface Parent {
    Set<Child> getChildren();
}
