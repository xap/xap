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
package {{project.groupId}};

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;

import java.time.LocalDate;

/**
 * A space class can be a simple POJO with getters and setters.
 * One property must be annotated with @SpaceId to define the primary key.
 * Additional properties may be indexed to boost query performance using @SpaceIndex
 * For more information on modeling space data see the documentation.
 */
public class Employee {
    private int id;
    private String name;
    private String title;
    private String department;
    private LocalDate birthday;
    private double salary;

    @Override
    public String toString() {
        return String.format("Employee [id=%s, name=%s, title=%s, department=%s, birthday=%s, salary=%s]",
                id, name, title, department, birthday, salary);
    }

    @SpaceId
    public int getId() {
        return id;
    }
    public Employee setId(int id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }
    public Employee setName(String name) {
        this.name = name;
        return this;
    }

    public String getTitle() {
        return title;
    }
    public Employee setTitle(String title) {
        this.title = title;
        return this;
    }

    @SpaceIndex
    public String getDepartment() {
        return department;
    }
    public Employee setDepartment(String department) {
        this.department = department;
        return this;
    }

    public LocalDate getBirthday() {
        return birthday;
    }
    public Employee setBirthday(LocalDate birthday) {
        this.birthday = birthday;
        return this;
    }

    public double getSalary() {
        return salary;
    }
    public Employee setSalary(double salary) {
        this.salary = salary;
        return this;
    }
}
