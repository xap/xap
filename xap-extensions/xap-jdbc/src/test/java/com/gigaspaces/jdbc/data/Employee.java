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
package com.gigaspaces.jdbc.data;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.sql.Time;
import java.sql.Timestamp;

@SpaceClass
public class Employee {
    private String id;
    private String first_name;
    private String last_name;
    private String email;
    private Integer age;
    private Short ageAsShort;
    private Double seniority;
    private java.sql.Date birthDateSQL;
    private Time birthTime;
    private Timestamp timestamp;
    private Long birthDateAsLong;
    private Boolean tenured;

    public Employee() {
    }

    public Employee(String id, String first_name, String last_name, String email, int age, Double seniority) {
        this.id = id;
        this.first_name = first_name;
        this.last_name = last_name;
        this.email = email;
        this.age = age;
        this.seniority = seniority;
    }
    public Employee(String id, String first_name, String last_name, String email, int age, Double seniority, Boolean tenured, java.util.Date dateWithTime, java.util.Date dateWithoutTime) {
        this(id, first_name, last_name, email, age, seniority);
        this.ageAsShort = Short.valueOf(Integer.toString(age));
        this.tenured = tenured;
        this.birthDateSQL = new java.sql.Date(dateWithoutTime.getTime());
        this.birthTime = Time.valueOf(new Time(dateWithTime.getTime()).toLocalTime());
        this.timestamp = new Timestamp(dateWithTime.getTime());
        this.birthDateAsLong = dateWithTime.getTime();
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

    @SpaceIndex(type = SpaceIndexType.EQUAL)
    @SpaceRouting
    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public Double getSeniority() {
        return seniority;
    }

    public void setSeniority(Double seniority) {
        this.seniority = seniority;
    }

    public java.sql.Date getBirthDateSQL() {
        return birthDateSQL;
    }

    public Employee setBirthDateSQL(java.sql.Date birthDateSQL) {
        this.birthDateSQL = birthDateSQL;
        return this;
    }

    public Time getBirthTime() {
        return birthTime;
    }

    public Employee setBirthTime(Time birthTime) {
        this.birthTime = birthTime;
        return this;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public Employee setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Long getBirthDateAsLong() {
        return birthDateAsLong;
    }

    public Employee setBirthDateAsLong(Long birthDateAsLong) {
        this.birthDateAsLong = birthDateAsLong;
        return this;
    }

    public Boolean getTenured() {
        return tenured;
    }

    public void setTenured(Boolean tenured) {
        this.tenured = tenured;
    }

    public Short getAgeAsShort() {
        return ageAsShort;
    }

    public void setAgeAsShort(Short ageAsShort) {
        this.ageAsShort = ageAsShort;
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
