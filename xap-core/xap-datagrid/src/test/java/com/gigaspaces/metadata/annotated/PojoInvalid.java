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

import com.gigaspaces.annotation.pojo.SpaceExclude;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpaceLeaseExpiration;
import com.gigaspaces.annotation.pojo.SpacePersist;
import com.gigaspaces.annotation.pojo.SpaceProperty;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.annotation.pojo.SpaceVersion;
import com.gigaspaces.metadata.index.SpaceIndexType;

/**
 * Container object for various invalid objects.
 *
 * @author Niv Ingberg
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class PojoInvalid {
    /**
     * Type must have a constructor with no parameters.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidNoDefaultConstructor {
        public InvalidNoDefaultConstructor(String foo) {
        }
    }

    /**
     * Type cannot have more than one id property.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidIdDuplicate {
        @SpaceId
        public String getProperty1() {
            return null;
        }

        public void setProperty1(String value) {
        }

        @SpaceId
        public String getProperty2() {
            return null;
        }

        public void setProperty2(String value) {
        }
    }

    /**
     * Type cannot have more than one lease expiration property.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidLeaseExpirationDuplicate {
        @SpaceLeaseExpiration
        public String getProperty1() {
            return null;
        }

        public void setProperty1(String value) {
        }

        @SpaceLeaseExpiration
        public String getProperty2() {
            return null;
        }

        public void setProperty2(String value) {
        }
    }

    /**
     * Type cannot have more than one persist property.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidPersistDuplicate {
        @SpacePersist
        public String getProperty1() {
            return null;
        }

        public void setProperty1(String value) {
        }

        @SpacePersist
        public String getProperty2() {
            return null;
        }

        public void setProperty2(String value) {
        }
    }

    /**
     * Type cannot have more than one routing property.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidRoutingDuplicate {
        @SpaceRouting
        public String getProperty1() {
            return null;
        }

        public void setProperty1(String value) {
        }

        @SpaceRouting
        public String getProperty2() {
            return null;
        }

        public void setProperty2(String value) {
        }
    }

    /**
     * Type cannot have more than one version property.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidVersionDuplicate {
        @SpaceVersion
        public String getProperty1() {
            return null;
        }

        public void setProperty1(String value) {
        }

        @SpaceVersion
        public String getProperty2() {
            return null;
        }

        public void setProperty2(String value) {
        }
    }

    /**
     * Lease expiration property type must be long or Long.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidLeaseExpirationType {
        @SpaceLeaseExpiration
        public String getProperty() {
            return null;
        }

        public void setProperty(String value) {
        }
    }

    /**
     * Persist property type must be boolean or Boolean.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidPersistType {
        @SpacePersist
        public String getProperty() {
            return null;
        }

        public void setProperty(String value) {
        }
    }

    /**
     * Version property type must be int or Integer.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidVersionType {
        @SpaceVersion
        public String getProperty() {
            return null;
        }

        public void setProperty() {
        }
    }

    /**
     * Id property with autogenerate=true type must be String.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidIdAutogenerateType {
        @SpaceId(autoGenerate = true)
        public int getProperty() {
            return -1;
        }

        public void setProperty(int value) {
        }
    }

    /**
     * Property cannot be both version and lease expiration.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidVersionWithLease {
        @SpaceVersion
        @SpaceLeaseExpiration
        public int getProperty() {
            return -1;
        }

        public void setProperty(int value) {
        }
    }

    /**
     * Property cannot be both version and persist.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidVersionWithPersist {
        @SpaceVersion
        @SpacePersist
        public int getProperty() {
            return -1;
        }

        public void setProperty(int value) {
        }
    }

    /**
     * Property cannot be both version and routing.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidVersionWithRouting {
        @SpaceVersion
        @SpaceRouting
        public int getProperty() {
            return -1;
        }

        public void setProperty(int value) {
        }
    }

    /**
     * Property cannot be both version and id.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidVersionWithId {
        @SpaceVersion
        @SpaceId
        public int getProperty() {
            return -1;
        }

        public void setProperty(int value) {
        }
    }

    /**
     * Property cannot be both version and space property.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidVersionWithSpaceProperty {
        @SpaceVersion
        @SpaceProperty
        public int getProperty() {
            return -1;
        }

        public void setProperty(int value) {
        }
    }

    /**
     * Property cannot be both lease expiration and persist.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidLeaseWithPersist {
        @SpaceLeaseExpiration
        @SpacePersist
        public long getProperty() {
            return -1;
        }

        public void setProperty(long value) {
        }
    }

    /**
     * Property cannot be both lease expiration and routing.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidLeaseWithRouting {
        @SpaceLeaseExpiration
        @SpaceRouting
        public long getProperty() {
            return -1;
        }

        public void setProperty(long value) {
        }
    }

    /**
     * Property cannot be both lease expiration and id.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidLeaseWithId {
        @SpaceLeaseExpiration
        @SpaceId
        public long getProperty() {
            return -1;
        }

        public void setProperty(long value) {
        }
    }

    /**
     * Property cannot be both lease expiration and space property.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidLeaseWithSpaceProperty {
        @SpaceLeaseExpiration
        @SpaceProperty
        public long getProperty() {
            return -1;
        }

        public void setProperty(long value) {
        }
    }

    /**
     * Property cannot be both persist and routing.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidPersistWithRouting {
        @SpacePersist
        @SpaceRouting
        public boolean getProperty() {
            return false;
        }

        public void setProperty(boolean value) {
        }
    }

    /**
     * Property cannot be both persist and id.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidPersistWithId {
        @SpacePersist
        @SpaceId
        public boolean getProperty() {
            return false;
        }

        public void setProperty(boolean value) {
        }
    }

    /**
     * Property cannot be both persist and space property.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidPersistWithSpaceProperty {
        @SpacePersist
        @SpaceProperty
        public boolean getProperty() {
            return false;
        }

        public void setProperty(boolean value) {
        }
    }

    /**
     * Routing property cannot be excluded.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidExcludeWithRouting {
        @SpaceExclude
        @SpaceRouting
        public boolean getProperty() {
            return false;
        }

        public void setProperty(boolean value) {
        }
    }

    /**
     * Id property cannot be excluded.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidExcludeWithId {
        @SpaceExclude
        @SpaceId
        public boolean getProperty() {
            return false;
        }

        public void setProperty(boolean value) {
        }
    }

    /**
     * Space property cannot be excluded.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidExcludeWithSpaceProperty {
        @SpaceExclude
        @SpaceProperty
        public boolean getProperty() {
            return false;
        }

        public void setProperty(boolean value) {
        }
    }

    public static class ValidWithProperty {
        @SpaceProperty
        public String getSomeValue() {
            return null;
        }

        public void setSomeValue(String value) {

        }
    }

    public static class InvalidExcludePropertyFromSuper extends ValidWithProperty {
        @Override
        @SpaceExclude
        public String getSomeValue() {
            return super.getSomeValue();
        }
    }

    /**
     * Space property cannot be read only.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidSpacePropertyWithoutSetter {
        @SpaceProperty
        public String getSomeProperty() {
            return null;
        }
    }

    /**
     * Space id cannot be read only.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidSpaceIdWithoutSetter

    {
        @SpaceId
        public String getSomeProperty() {
            return null;
        }
    }

    /**
     * Space routing cannot be read only.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidSpaceRoutingWithoutSetter {
        @SpaceRouting
        public String getSomeProperty() {
            return null;
        }
    }

    /**
     * Space version cannot be read only.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidSpaceVersionWithoutSetter {
        @SpaceVersion
        public int getSomeProperty() {
            return -1;
        }
    }

    /**
     * Space persist cannot be read only.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidSpacePersistWithoutSetter {
        @SpacePersist
        public boolean getSomeProperty() {
            return false;
        }
    }

    /**
     * Space lease expiration cannot be read only.
     *
     * @author Niv Ingberg
     * @since 7.0.1
     */
    public static class InvalidSpaceLeaseExpirationWithoutSetter {
        @SpaceLeaseExpiration
        public long getSomeProperty() {
            return -1;
        }
    }

    /**
     * An indexed space property cannot be excluded.
     *
     * @author idan
     * @since 7.1.1
     */
    public static class InvalidExcludePropertyWithIndex {

        @SpaceIndex(type = SpaceIndexType.EQUAL)
        @SpaceExclude
        public Integer getIndexedField() {
            return 0;
        }
    }
}
