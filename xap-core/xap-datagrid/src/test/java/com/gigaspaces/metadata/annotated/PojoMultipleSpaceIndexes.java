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

package com.gigaspaces.metadata.annotated;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpaceIndexes;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.io.Serializable;

//import javax.persistence.Embedded;

/**
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class PojoMultipleSpaceIndexes {

    private int id;

    private Info personalInfo;

    private String description;

    public PojoMultipleSpaceIndexes() {

    }

    @SpaceId(autoGenerate = false)
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @SpaceIndexes({
            @SpaceIndex(type = SpaceIndexType.EQUAL),
            @SpaceIndex(path = "name", type = SpaceIndexType.EQUAL),
            @SpaceIndex(path = "address.zipCode", type = SpaceIndexType.EQUAL_AND_ORDERED)})
    public Info getPersonalInfo() {
        return personalInfo;
    }

    public void setPersonalInfo(Info personalInfo) {
        this.personalInfo = personalInfo;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String desc) {
        this.description = desc;
    }


    public static class Info implements Serializable {
        private String name;

        private Address address;

        /**
         *
         */
        public Info() {
            super();

        }

        public Info(String name, Address address) {
            super();
            this.name = name;
            this.address = address;
        }

        //@Embedded
        public Address getAddress() {
            return address;
        }

        public void setAddress(Address address) {
            this.address = address;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

    }

    public static class Address implements Serializable {
        private int zipCode;
        private String street;

        public Address() {

        }

        /**
         * @param zipCode
         * @param street
         */
        public Address(int zipCode, String street) {
            super();
            this.zipCode = zipCode;
            this.street = street;
        }

        public int getZipCode() {
            return zipCode;
        }

        public String getStreet() {
            return street;
        }

        public void setZipCode(int zipCode) {
            this.zipCode = zipCode;
        }

        public void setStreet(String street) {
            this.street = street;
        }

    }
}

