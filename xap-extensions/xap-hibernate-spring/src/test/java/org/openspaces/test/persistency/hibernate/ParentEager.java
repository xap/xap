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
package org.openspaces.test.persistency.hibernate;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Set;

@Entity
public class ParentEager implements Parent {
    @Id
    private String name;
    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    private Set<Child> children = new HashSet<>();

    public ParentEager() {}
    public ParentEager(String name, String ... children) {
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
