package com.gigaspaces.jdbc.data;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;

@SpaceClass
public class Product {
    private String id;
    private String name;
    private String department;

    public Product() {
    }

    public Product(String id, String name, String department) {
        this.id = id;
        this.name = name;
        this.department = department;
    }

    @SpaceId
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }
}
