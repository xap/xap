package com.gigaspaces.jdbc.data;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

@SpaceClass
public class AutoGeneratedEmployee {
    private String id;
    private String first_name;
    private String last_name;
    private String email;
    private Integer age;
    private Double seniority;
    private Date birthDate;
    private java.sql.Date birthDateSQL;
    private Time birthTime;
    private Timestamp timestamp;
    private Long birthDateAsLong;

    public AutoGeneratedEmployee() {
    }

    public AutoGeneratedEmployee(String first_name, String last_name, String email, int age, Double seniority) {
        this.first_name = first_name;
        this.last_name = last_name;
        this.email = email;
        this.age = age;
        this.seniority = seniority;
    }

    @SpaceId(autoGenerate = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public String getFirst_name() {
        return first_name;
    }

    public void setFirst_name(String first_name) {
        this.first_name = first_name;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public String getLast_name() {
        return last_name;
    }

    public void setLast_name(String last_name) {
        this.last_name = last_name;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Double getSeniority() {
        return seniority;
    }

    public void setSeniority(Double seniority) {
        this.seniority = seniority;
    }

    public Date getBirthDate() {
        return birthDate;
    }

    public AutoGeneratedEmployee setBirthDate(Date birthDate) {
        this.birthDate = birthDate;
        return this;
    }

    public java.sql.Date getBirthDateSQL() {
        return birthDateSQL;
    }

    public AutoGeneratedEmployee setBirthDateSQL(java.sql.Date birthDateSQL) {
        this.birthDateSQL = birthDateSQL;
        return this;
    }

    public Time getBirthTime() {
        return birthTime;
    }

    public AutoGeneratedEmployee setBirthTime(Time birthTime) {
        this.birthTime = birthTime;
        return this;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public AutoGeneratedEmployee setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Long getBirthDateAsLong() {
        return birthDateAsLong;
    }

    public AutoGeneratedEmployee setBirthDateAsLong(Long birthDateAsLong) {
        this.birthDateAsLong = birthDateAsLong;
        return this;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "id='" + id + '\'' +
                ", first_name='" + first_name + '\'' +
                ", last_name='" + last_name + '\'' +
                ", email='" + email + '\'' +
                ", age=" + age +
                ", seniority=" + seniority +
                '}';
    }
}
