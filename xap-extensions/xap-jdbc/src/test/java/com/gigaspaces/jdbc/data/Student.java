package com.gigaspaces.jdbc.data;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.metadata.index.SpaceIndexType;

@SpaceClass
public class Student {
    private String id;
    private String first_name;
    private String last_name;
    private String email;
    private Integer age;
    private Short grade;
    private String car_id;
    private Integer driving_license_id;

    public Student() {
    }

    public Student(String id, String first_name, String last_name, String email, int age, Short grade) {
        this( id, first_name, last_name, email, age, grade, null, null );
    }

    public Student(String id, String first_name, String last_name, String email, int age, Short grade, String car_id, Integer driving_license_id) {
        this.id = id;
        this.first_name = first_name;
        this.last_name = last_name;
        this.email = email;
        this.age = age;
        this.grade = grade;
        this.car_id = car_id;
        this.driving_license_id = driving_license_id;
    }

    @SpaceId
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

    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    @SpaceRouting
    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public Short getGrade() {
        return grade;
    }

    public void setGrade(Short grade) {
        this.grade = grade;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public String getCar_id() {
        return car_id;
    }

    public void setCar_id(String car_id) {
        this.car_id = car_id;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    public Integer getDriving_license_id() {
        return driving_license_id;
    }

    public void setDriving_license_id(Integer driving_license_id) {
        this.driving_license_id = driving_license_id;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id='" + id + '\'' +
                ", first_name='" + first_name + '\'' +
                ", last_name='" + last_name + '\'' +
                ", email='" + email + '\'' +
                ", age=" + age +
                ", grade=" + grade +
                ", car_id=" + car_id +
                ", driving_license_id=" + driving_license_id +
                '}';
    }
}