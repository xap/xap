package com.gigaspaces.jdbc;

import com.gigaspaces.annotation.pojo.SpaceId;

public class MyPojo {
    private String id;
    private String name;
    private Integer age;
    private String country;

    public MyPojo() {
    }

    public MyPojo(String name, int age, String country) {
        this.name = name;
        this.age = age;
        this.country = country;
    }

    @SpaceId(autoGenerate = true)
    public String getId() {
        return id;
    }

    public MyPojo setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public MyPojo setName(String name) {
        this.name = name;
        return this;
    }

    public Integer getAge() {
        return age;
    }

    public MyPojo setAge(Integer age) {
        this.age = age;
        return this;
    }

    public String getCountry() {
        return country;
    }

    public MyPojo setCountry(String country) {
        this.country = country;
        return this;
    }

    @Override
    public String toString() {
        return "MyPojo{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", country='" + country + '\'' +
                '}';
    }
}
