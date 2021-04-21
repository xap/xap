package com.gigaspaces.jdbc;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

public class MyPojo {
    private String id;
    private String name;
    private String first_name;
    private String last_name;
    private String email;
    private Integer age;
    private String country;
    private Date birthDate;
    private Time birthTime;
    private Timestamp timestamp;
    private Long birthLong;

    public MyPojo() {
    }
    public MyPojo(String name, Integer age, String country, Date birthDate, Time birthTime, Timestamp timestamp) {
        this(name.split(" ")[0], name.split(" ")[1], name.split(" ")[0]+"@msn.com", age, country, birthDate, birthTime, timestamp);
    }
    public MyPojo(String first_name, String last_name, String email, Integer age, String country, Date birthDate, Time birthTime, Timestamp timestamp) {
        this.first_name = first_name;
        this.name = first_name;
        this.last_name = last_name;
        this.email = email;
        this.age = age;
        this.country = country;
        this.birthDate = birthDate;
        this.birthTime = birthTime;
        this.timestamp = timestamp;
        this.birthLong = birthDate.getTime();
    }

    @SpaceId(autoGenerate = true)
    public String getId() {
        return id;
    }

    public MyPojo setId(String id) {
        this.id = id;
        return this;
    }

    @SpaceIndex
    public String getFirst_name() {
        return first_name;
    }

    public String getLast_name() {
        return last_name;
    }

    public String getEmail() {
        return email;
    }

    public MyPojo setFirst_name(String first_name) {
        this.first_name = first_name;
        return this;
    }

    public MyPojo setLast_name(String last_name) {
        this.last_name = last_name;
        return this;
    }

    public MyPojo setEmail(String email) {
        this.email = email;
        return this;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
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

    public Date getBirthDate() {
        return birthDate;
    }

    public MyPojo setBirthDate(Date birthDate) {
        this.birthDate = birthDate;
        return this;
    }

    public Time getBirthTime() {
        return birthTime;
    }

    public MyPojo setBirthTime(Time birthTime) {
        this.birthTime = birthTime;
        return this;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public MyPojo setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Long getBirthLong() {
        return birthLong;
    }

    public MyPojo setBirthLong(Long birthLong) {
        this.birthLong = birthLong;
        return this;
    }

    public String getName() {
        return name;
    }

    public MyPojo setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public String toString() {
        return "MyPojo{" +
                "id='" + id + '\'' +
                ", first_name='" + first_name + '\'' +
                ", last_name='" + last_name + '\'' +
                ", email='" + email + '\'' +
                ", age=" + age +
                ", country='" + country + '\'' +
                ", birthDate=" + birthDate +
                ", birthTime=" + birthTime +
                ", timestamp=" + timestamp +
                ", birthLong=" + birthLong +
                '}';
    }
}
