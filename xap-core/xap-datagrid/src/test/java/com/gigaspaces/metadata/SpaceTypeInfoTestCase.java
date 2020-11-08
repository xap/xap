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

package com.gigaspaces.metadata;

import com.gigaspaces.internal.exceptions.IllegalArgumentNullException;
import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryPathGetter;
import com.gigaspaces.metadata.annotated.PojoAttributes;
import com.gigaspaces.metadata.annotated.PojoAttributesInheritence1;
import com.gigaspaces.metadata.annotated.PojoAttributesInheritence2;
import com.gigaspaces.metadata.annotated.PojoCommonProperties;
import com.gigaspaces.metadata.annotated.PojoCommonPropertiesInheritence;
import com.gigaspaces.metadata.annotated.PojoCustomIndex;
import com.gigaspaces.metadata.annotated.PojoCustomIndexes;
import com.gigaspaces.metadata.annotated.PojoExplicit;
import com.gigaspaces.metadata.annotated.PojoExplicitExDefault;
import com.gigaspaces.metadata.annotated.PojoExplicitExExplicit;
import com.gigaspaces.metadata.annotated.PojoExplicitExImplicit;
import com.gigaspaces.metadata.annotated.PojoIdAutogenerateIndexExplicit;
import com.gigaspaces.metadata.annotated.PojoIdAutogenerateIndexImplicit;
import com.gigaspaces.metadata.annotated.PojoIdAutogenerateIndexImplicitInherit;
import com.gigaspaces.metadata.annotated.PojoIdIndexExplicit;
import com.gigaspaces.metadata.annotated.PojoIdIndexImplicit;
import com.gigaspaces.metadata.annotated.PojoIllegalCustomIndexes;
import com.gigaspaces.metadata.annotated.PojoIllegalDuplicateSpaceIndex;
import com.gigaspaces.metadata.annotated.PojoIllegalDuplicateSpaceIndex2;
import com.gigaspaces.metadata.annotated.PojoIllegalDuplicateSpaceIndex3;
import com.gigaspaces.metadata.annotated.PojoImplicit;
import com.gigaspaces.metadata.annotated.PojoImplicitExDefault;
import com.gigaspaces.metadata.annotated.PojoImplicitExExplicit;
import com.gigaspaces.metadata.annotated.PojoImplicitExImplicit;
import com.gigaspaces.metadata.annotated.PojoIndexes;
import com.gigaspaces.metadata.annotated.PojoIndexesInheritFalse;
import com.gigaspaces.metadata.annotated.PojoIndexesInheritTrue;
import com.gigaspaces.metadata.annotated.PojoInheritenceSort1;
import com.gigaspaces.metadata.annotated.PojoInheritenceSort2;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidExcludePropertyFromSuper;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidExcludePropertyWithIndex;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidExcludeWithId;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidExcludeWithRouting;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidExcludeWithSpaceProperty;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidIdAutogenerateType;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidIdDuplicate;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidLeaseExpirationDuplicate;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidLeaseExpirationType;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidLeaseWithId;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidLeaseWithPersist;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidLeaseWithRouting;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidLeaseWithSpaceProperty;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidPersistDuplicate;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidPersistType;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidPersistWithId;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidPersistWithRouting;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidPersistWithSpaceProperty;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidRoutingDuplicate;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidSpaceIdWithoutSetter;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidSpaceLeaseExpirationWithoutSetter;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidSpacePersistWithoutSetter;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidSpacePropertyWithoutSetter;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidSpaceRoutingWithoutSetter;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidSpaceVersionWithoutSetter;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidVersionDuplicate;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidVersionType;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidVersionWithId;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidVersionWithLease;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidVersionWithPersist;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidVersionWithRouting;
import com.gigaspaces.metadata.annotated.PojoInvalid.InvalidVersionWithSpaceProperty;
import com.gigaspaces.metadata.annotated.PojoMultipleSpaceIndexes;
import com.gigaspaces.metadata.annotated.PojoRoutingIndexExplicit;
import com.gigaspaces.metadata.annotated.PojoRoutingIndexImplicit;
import com.gigaspaces.metadata.annotated.PojoSpaceIndex;
import com.gigaspaces.metadata.annotated.PojoSpacePropertyInherit;
import com.gigaspaces.metadata.gsxml.PojoAnnotatedAndXmled;
import com.gigaspaces.metadata.gsxml.PojoBasicStorageTypeXml;
import com.gigaspaces.metadata.gsxml.PojoDuplicateSpaceIndexXml;
import com.gigaspaces.metadata.gsxml.PojoIllegalSpaceClassStorageTypeXml;
import com.gigaspaces.metadata.gsxml.PojoSpaceClassStorageTypeViaXmlBinaryType;
import com.gigaspaces.metadata.gsxml.PojoSpaceClassStorageTypeViaXmlCompressedType;
import com.gigaspaces.metadata.gsxml.PojoSpaceClassStorageTypeViaXmlDefaultType;
import com.gigaspaces.metadata.gsxml.PojoSpaceClassStorageTypeViaXmlObjectType;
import com.gigaspaces.metadata.gsxml.PojoSpaceIndexXml;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.metadata.pojos.PojoNoProperties;
import com.gigaspaces.metadata.pojos.PojoOneProperty;
import com.gigaspaces.metadata.pojos.PojoPropertiesStorageType;
import com.gigaspaces.metadata.pojos.PojoSettersOnly;
import com.gigaspaces.metadata.pojos.PojoSpaceClassStorageType;
import com.gigaspaces.metadata.pojos.PojoSpaceClassStorageTypeInherit;
import com.gigaspaces.metadata.pojos.PojoSpaceClassStorageTypeInheritAndOverride;

import junit.framework.TestCase;
import org.junit.Assert;

import java.lang.reflect.Method;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class SpaceTypeInfoTestCase extends TestCase {

    @Override
    protected void setUp() throws Exception {
        System.setProperty("com.gs.binary.enabled", "false");
        super.setUp();
    }


    public void testNull() {
        // Arrange:
        final Class<?> type = null;

        // Act/Assert:
        try {
            SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);
            Assert.fail("Expected IllegalArgumentNullException.");
        } catch (IllegalArgumentNullException e) {
        }
    }

    public void testPojoSettersOnly() {
        // Arrange:
        final Class<?> type = PojoSettersOnly.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 2, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, "foo", String.class, false, true);
        Assert.assertEquals("numOfSpaceProperties", 0, typeInfo.getNumOfSpaceProperties());
    }

    public void testObject() {
        // Arrange:
        final Class<?> type = Object.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 1, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 0, typeInfo.getNumOfSpaceProperties());
    }

    public void testPojoNoProperties() {
        // Arrange:
        final Class<?> type = PojoNoProperties.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 1, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 0, typeInfo.getNumOfSpaceProperties());
    }

    public void testPojoOneProperty() {
        // Arrange:
        final Class<?> type = PojoOneProperty.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 2, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "name", String.class, 0);
    }

    public void testPojoCommonProperties() {
        // Arrange:
        final Class<?> type = PojoCommonProperties.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 12, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, "someReadonlyProperty", int.class, true, false);
        assertPropertyInfo(typeInfo, "someWriteonlyProperty", int.class, false, true);
        Assert.assertEquals("numOfSpaceProperties", 9, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "someBoolean", boolean.class, 0, SpaceIndexType.NONE, true);
        assertPropertyInfo(typeInfo, 1, "someBooleanWrapper", Boolean.class, 0);
        assertPropertyInfo(typeInfo, 2, "someInt", int.class, 0, SpaceIndexType.NONE, -1);
        assertPropertyInfo(typeInfo, 3, "someIntWrapper", Integer.class, 0);
        assertPropertyInfo(typeInfo, 4, "someLong", long.class, 0, SpaceIndexType.NONE, -10L);
        assertPropertyInfo(typeInfo, 5, "someLongWrapper", Long.class, 0);
        assertPropertyInfo(typeInfo, 6, "someObject", Object.class, 0);
        assertPropertyInfo(typeInfo, 7, "someOverload", String.class, 0);
        assertPropertyInfo(typeInfo, 8, "someString", String.class, 0);
    }

    public void testPojoCommonPropertiesInheritence() {
        // Arrange:
        final Class<?> type = PojoCommonPropertiesInheritence.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 12, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, "someReadonlyProperty", int.class, true, false);
        assertPropertyInfo(typeInfo, "someWriteonlyProperty", int.class, false, true);
        Assert.assertEquals("numOfSpaceProperties", 9, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "someBoolean", boolean.class, 1, SpaceIndexType.NONE, true);
        assertPropertyInfo(typeInfo, 1, "someBooleanWrapper", Boolean.class, 1);
        assertPropertyInfo(typeInfo, 2, "someInt", int.class, 1, SpaceIndexType.NONE, -2);
        assertPropertyInfo(typeInfo, 3, "someIntWrapper", Integer.class, 1);
        assertPropertyInfo(typeInfo, 4, "someLong", long.class, 1, SpaceIndexType.NONE, -10L);
        assertPropertyInfo(typeInfo, 5, "someLongWrapper", Long.class, 1);
        assertPropertyInfo(typeInfo, 6, "someObject", Object.class, 1);
        assertPropertyInfo(typeInfo, 7, "someOverload", String.class, 1);
        assertPropertyInfo(typeInfo, 8, "someString", String.class, 1);
    }

    public void testPojoInheritenceSort1() {
        // Arrange:
        final Class<?> type = PojoInheritenceSort1.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 11, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, "fieldB1", String.class, true, false);
        assertPropertyInfo(typeInfo, "fieldB2", String.class, false, true);
        assertPropertyInfo(typeInfo, "fieldD5", String.class, true, true);
        Assert.assertEquals("numOfSpaceProperties", 7, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "fieldB", String.class, 0);
        assertPropertyInfo(typeInfo, 1, "fieldD", String.class, 0);
        assertPropertyInfo(typeInfo, 2, "fieldD1", String.class, 0);
        assertPropertyInfo(typeInfo, 3, "fieldD2", String.class, 0);
        assertPropertyInfo(typeInfo, 4, "fieldD3", String.class, 0);
        assertPropertyInfo(typeInfo, 5, "fieldD4", String.class, 0, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 6, "fieldD6", String.class, 0, SpaceIndexType.EQUAL);
    }

    public void testPojoInheritenceSort2() {
        // Arrange:
        final Class<?> type = PojoInheritenceSort2.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 14, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 13, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "fieldB", String.class, 1);
        assertPropertyInfo(typeInfo, 1, "fieldD", String.class, 1);
        assertPropertyInfo(typeInfo, 2, "fieldD1", String.class, 1);
        assertPropertyInfo(typeInfo, 3, "fieldD2", String.class, 1);
        assertPropertyInfo(typeInfo, 4, "fieldD3", String.class, 1);
        assertPropertyInfo(typeInfo, 5, "fieldD4", String.class, 1, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 6, "fieldD6", String.class, 1, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 7, "fieldA", String.class, 0);
        assertPropertyInfo(typeInfo, 8, "fieldB1", String.class, 0);
        assertPropertyInfo(typeInfo, 9, "fieldB2", String.class, 0);
        assertPropertyInfo(typeInfo, 10, "fieldC", String.class, 0);
        assertPropertyInfo(typeInfo, 11, "fieldD5", String.class, 0);
        assertPropertyInfo(typeInfo, 12, "fieldE", String.class, 0);
    }

    public void testPojoImplicit() {
        // Arrange:
        final Class<?> type = PojoImplicit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 4, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, "excludedProperty", String.class);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class, 0);
        assertPropertyInfo(typeInfo, 1, "implicitProperty", String.class, 0);
    }

    public void testPojoImplicitExDefault() {
        // Arrange:
        final Class<?> type = PojoImplicitExDefault.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 4, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, "excludedProperty", String.class);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class, 1);
        assertPropertyInfo(typeInfo, 1, "implicitProperty", String.class, 1);
    }

    public void testPojoImplicitExEcplicit() {
        // Arrange:
        final Class<?> type = PojoImplicitExExplicit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 4, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, "excludedProperty", String.class);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class, 1);
        assertPropertyInfo(typeInfo, 1, "implicitProperty", String.class, 1);
    }

    public void testPojoImplicitExImplicit() {
        // Arrange:
        final Class<?> type = PojoImplicitExImplicit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 4, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, "excludedProperty", String.class);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class, 1);
        assertPropertyInfo(typeInfo, 1, "implicitProperty", String.class, 1);
    }

    public void testPojoExplicit() {
        // Arrange:
        final Class<?> type = PojoExplicit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 6, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class, 0);
        assertPropertyInfo(typeInfo, 1, "pojoId", Integer.class, 0, SpaceIndexType.EQUAL, null, true, true);
        assertPropertyInfo(typeInfo, 2, "pojoIndexedField", Integer.class, 0, SpaceIndexType.EQUAL, null, true, true);
        assertPropertyInfo(typeInfo, "excludedProperty", String.class);
        assertPropertyInfo(typeInfo, "implicitProperty", String.class);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfSpaceProperties());
    }

    public void testPojoExplicitExDefault() {
        // Arrange:
        final Class<?> type = PojoExplicitExDefault.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 6, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class, 1);
        assertPropertyInfo(typeInfo, 1, "pojoId", Integer.class, 1, SpaceIndexType.EQUAL, null, true, true);
        assertPropertyInfo(typeInfo, 2, "pojoIndexedField", Integer.class, 1, SpaceIndexType.EQUAL, null, true, true);
        assertPropertyInfo(typeInfo, "excludedProperty", String.class);
        assertPropertyInfo(typeInfo, "implicitProperty", String.class);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfSpaceProperties());
    }

    public void testPojoExplicitExExplicit() {
        // Arrange:
        final Class<?> type = PojoExplicitExExplicit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 6, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class, 1);
        assertPropertyInfo(typeInfo, 1, "pojoId", Integer.class, 1, SpaceIndexType.EQUAL, null, true, true);
        assertPropertyInfo(typeInfo, 2, "pojoIndexedField", Integer.class, 1, SpaceIndexType.EQUAL, null, true, true);
        assertPropertyInfo(typeInfo, -1, "excludedProperty", String.class, -1);
        assertPropertyInfo(typeInfo, -1, "implicitProperty", String.class, -1);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfSpaceProperties());
    }

    public void testPojoExplicitExImplicit() {
        // Arrange:
        final Class<?> type = PojoExplicitExImplicit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 6, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class, 1);
        assertPropertyInfo(typeInfo, 1, "pojoId", Integer.class, 1, SpaceIndexType.EQUAL, null, true, true);
        assertPropertyInfo(typeInfo, 2, "pojoIndexedField", Integer.class, 1, SpaceIndexType.EQUAL, null, true, true);
        assertPropertyInfo(typeInfo, "excludedProperty", String.class);
        assertPropertyInfo(typeInfo, "implicitProperty", String.class);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfSpaceProperties());
    }

    public void testPojoAttributes() {
        // Arrange:
        final Class<?> type = PojoAttributes.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 6, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, "version", int.class);
        assertPropertyInfo(typeInfo, "leaseExpiration", long.class);
        assertPropertyInfo(typeInfo, "persist", boolean.class);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class, 0, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 1, "routing", String.class, 0, SpaceIndexType.EQUAL);

        Assert.assertEquals("id", typeInfo.getProperty("id"), typeInfo.getIdProperty());
        Assert.assertEquals("leaseExpiration", typeInfo.getProperty("leaseExpiration"), typeInfo.getLeaseExpirationProperty());
        Assert.assertEquals("persist", typeInfo.getProperty("persist"), typeInfo.getPersistProperty());
        Assert.assertEquals("routing", typeInfo.getProperty("routing"), typeInfo.getRoutingProperty());
        Assert.assertEquals("version", typeInfo.getProperty("version"), typeInfo.getVersionProperty());
    }

    public void testPojoAttributesInheritence1() {
        // Arrange:
        final Class<?> type = PojoAttributesInheritence1.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 6, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, "version", int.class);
        assertPropertyInfo(typeInfo, "leaseExpiration", long.class);
        assertPropertyInfo(typeInfo, "persist", boolean.class);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class, 1, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 1, "routing", String.class, 1, SpaceIndexType.EQUAL);

        Assert.assertEquals("id", typeInfo.getProperty("id"), typeInfo.getIdProperty());
        Assert.assertEquals("leaseExpiration", typeInfo.getProperty("leaseExpiration"), typeInfo.getLeaseExpirationProperty());
        Assert.assertEquals("persist", typeInfo.getProperty("persist"), typeInfo.getPersistProperty());
        Assert.assertEquals("routing", typeInfo.getProperty("routing"), typeInfo.getRoutingProperty());
        Assert.assertEquals("version", typeInfo.getProperty("version"), typeInfo.getVersionProperty());
    }

    public void testPojoAttributesInheritence2() {
        // Arrange:
        final Class<?> type = PojoAttributesInheritence2.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 11, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        assertPropertyInfo(typeInfo, "version", int.class);
        assertPropertyInfo(typeInfo, "leaseExpiration", long.class);
        assertPropertyInfo(typeInfo, "persist", boolean.class);
        assertPropertyInfo(typeInfo, "version2", int.class);
        assertPropertyInfo(typeInfo, "leaseExpiration2", long.class);
        assertPropertyInfo(typeInfo, "persist2", boolean.class);
        Assert.assertEquals("numOfSpaceProperties", 4, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class, 1, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 1, "routing", String.class, 1, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 2, "id2", String.class, 0, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 3, "routing2", String.class, 0, SpaceIndexType.EQUAL);

        Assert.assertEquals("id", typeInfo.getProperty("id2"), typeInfo.getIdProperty());
        Assert.assertEquals("leaseExpiration", typeInfo.getProperty("leaseExpiration2"), typeInfo.getLeaseExpirationProperty());
        Assert.assertEquals("persist", typeInfo.getProperty("persist2"), typeInfo.getPersistProperty());
        Assert.assertEquals("routing", typeInfo.getProperty("routing2"), typeInfo.getRoutingProperty());
        Assert.assertEquals("version", typeInfo.getProperty("version2"), typeInfo.getVersionProperty());
    }

    public void testPojoRoutingIndexImplicit() {
        // Arrange:
        final Class<?> type = PojoRoutingIndexImplicit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 2, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "routing", String.class, 0, SpaceIndexType.EQUAL);
    }

    public void testPojoRoutingIndexExplicit() {
        // Arrange:
        final Class<?> type = PojoRoutingIndexExplicit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 2, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "routing", String.class, 0, SpaceIndexType.NONE);
        Assert.assertEquals("idAutoGenerate", false, typeInfo.getIdAutoGenerate());
    }

    public void testPojoIdIndexImplicit() {
        // Arrange:
        final Class<?> type = PojoIdIndexImplicit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 2, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class, 0, SpaceIndexType.EQUAL);
        Assert.assertEquals("idAutoGenerate", false, typeInfo.getIdAutoGenerate());
    }

    public void testPojoIdIndexExplicit() {
        // Arrange:
        final Class<?> type = PojoIdIndexExplicit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 2, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class, 0, SpaceIndexType.NONE);
    }

    public void testPojoIdAutogenerateIndexImplicit() {
        // Arrange:
        final Class<?> type = PojoIdAutogenerateIndexImplicit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 2, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class, 0, SpaceIndexType.NONE);
        Assert.assertEquals("idAutoGenerate", true, typeInfo.getIdAutoGenerate());
    }

    public void testPojoIdAutogenerateIndexExplicit() {
        // Arrange:
        final Class<?> type = PojoIdAutogenerateIndexExplicit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 2, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class, 0, SpaceIndexType.EQUAL);
        Assert.assertEquals("idAutoGenerate", true, typeInfo.getIdAutoGenerate());
    }

    public void testPojoIdAutogenerateIndexImplicitInherit() {
        // Arrange:
        final Class<?> type = PojoIdAutogenerateIndexImplicitInherit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 2, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class, 1, SpaceIndexType.NONE);
        Assert.assertEquals("idAutoGenerate", true, typeInfo.getIdAutoGenerate());
    }

    public void testPojoIndexes() {
        // Arrange:
        final Class<?> type = PojoIndexes.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 5, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 4, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "valueBasic", String.class, 0, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 1, "valueDefault", String.class, 0, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 2, "valueExtended", String.class, 0, SpaceIndexType.EQUAL_AND_ORDERED);
        assertPropertyInfo(typeInfo, 3, "valueNone", String.class, 0, SpaceIndexType.NONE);
    }

    public void testPojoIndexesInheritTrue() {
        // Arrange:
        final Class<?> type = PojoIndexesInheritTrue.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 9, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 8, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "valueBasic", String.class, 1, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 1, "valueDefault", String.class, 1, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 2, "valueExtended", String.class, 1, SpaceIndexType.EQUAL_AND_ORDERED);
        assertPropertyInfo(typeInfo, 3, "valueNone", String.class, 1, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 4, "valueBasic2", String.class, 0, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 5, "valueDefault2", String.class, 0, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 6, "valueExtended2", String.class, 0, SpaceIndexType.EQUAL_AND_ORDERED);
        assertPropertyInfo(typeInfo, 7, "valueNone2", String.class, 0, SpaceIndexType.NONE);
    }

    public void testPojoIndexesInheritFalse() {
        // Arrange:
        final Class<?> type = PojoIndexesInheritFalse.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 9, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 8, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "valueBasic", String.class, 1, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 1, "valueDefault", String.class, 1, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 2, "valueExtended", String.class, 1, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 3, "valueNone", String.class, 1, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 4, "valueBasic2", String.class, 0, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 5, "valueDefault2", String.class, 0, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 6, "valueExtended2", String.class, 0, SpaceIndexType.EQUAL_AND_ORDERED);
        assertPropertyInfo(typeInfo, 7, "valueNone2", String.class, 0, SpaceIndexType.NONE);
    }

    public void testPojoPojoAnnotatedAndXmled() {
        // Arrange:
        final Class<?> type = PojoAnnotatedAndXmled.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 4, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "someProperty1", String.class, 0, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 1, "someProperty2", String.class, 0, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 2, "someProperty3", String.class, 0, SpaceIndexType.EQUAL);
        Assert.assertEquals("id", typeInfo.getProperty("someProperty2"), typeInfo.getIdProperty());
        Assert.assertEquals("idAutoGenerate", true, typeInfo.getIdAutoGenerate());
    }

    public void testInvalidPojos() {
        // Arrange:
        Class<?>[] types = new Class<?>[]
                {
                        InvalidPersistType.class,
                        InvalidLeaseExpirationType.class,
                        InvalidVersionType.class,
                        InvalidIdAutogenerateType.class,

                        InvalidPersistDuplicate.class,
                        InvalidLeaseExpirationDuplicate.class,
                        InvalidVersionDuplicate.class,
                        InvalidIdDuplicate.class,
                        InvalidRoutingDuplicate.class,

                        InvalidVersionWithLease.class,
                        InvalidVersionWithPersist.class,
                        InvalidVersionWithRouting.class,
                        InvalidVersionWithId.class,
                        InvalidVersionWithSpaceProperty.class,

                        InvalidLeaseWithPersist.class,
                        InvalidLeaseWithRouting.class,
                        InvalidLeaseWithId.class,
                        InvalidLeaseWithSpaceProperty.class,

                        InvalidPersistWithRouting.class,
                        InvalidPersistWithId.class,
                        InvalidPersistWithSpaceProperty.class,

                        InvalidExcludeWithRouting.class,
                        InvalidExcludeWithId.class,
                        InvalidExcludeWithSpaceProperty.class,

                        InvalidExcludePropertyFromSuper.class,
                        InvalidExcludePropertyWithIndex.class,

                        InvalidSpacePropertyWithoutSetter.class,
                        InvalidSpaceIdWithoutSetter.class,
                        InvalidSpaceRoutingWithoutSetter.class,
                        InvalidSpaceVersionWithoutSetter.class,
                        InvalidSpacePersistWithoutSetter.class,
                        InvalidSpaceLeaseExpirationWithoutSetter.class,
                };

        // Act\Assert:
        for (Class<?> type : types) {
            try {
                SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);
                Assert.fail("Should have thrown an exception for type " + type.getName());
            } catch (SpaceMetadataValidationException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public void testInheritSpaceProperty() {
        // Arrange:
        final Class<?> type = PojoSpacePropertyInherit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 3, typeInfo.getNumOfProperties());
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "value", int.class, 1, SpaceIndexType.EQUAL, 0, true, true);
    }

    public void testInheritOverrideNullValue() {
        // Arrange:
        final Class<?> type = PojoSpacePropertyInherit.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 3, typeInfo.getNumOfProperties());
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 1, "value2", int.class, 1, SpaceIndexType.NONE, -1, true, true);
    }

    private void assertSpaceTypeInfo(SpaceTypeInfo typeInfo, Class<?> type) {
        Assert.assertEquals("getType", type, typeInfo.getType());
        Assert.assertEquals("getName", type.getName(), typeInfo.getName());

        Class<?> superType = type.getSuperclass();
        if (superType == null)
            Assert.assertEquals(null, typeInfo.getSuperTypeInfo());
        else
            assertSpaceTypeInfo(typeInfo.getSuperTypeInfo(), superType);
    }

    private void assertPropertyInfo(SpaceTypeInfo typeInfo, String name, Class<?> type, StorageType storageType) {
        SpacePropertyInfo property = typeInfo.getProperty(name);
        Assert.assertNotNull("property [" + name + "]", property);
        Assert.assertEquals("name", name, property.getName());
        Assert.assertEquals("type", type, property.getType());
        Assert.assertEquals("storage type", storageType, property.getStorageType());
    }

    private void assertPropertyInfo(SpaceTypeInfo typeInfo, String name, Class<?> type) {
        assertPropertyInfo(typeInfo, -1, name, type, -1, null, null, true, true);
    }

    private void assertPropertyInfo(SpaceTypeInfo typeInfo, String name, Class<?> type,
                                    boolean hasGetter, boolean hasSetter) {
        assertPropertyInfo(typeInfo, -1, name, type, -1, null, null, hasGetter, hasSetter);
    }

    private void assertPropertyInfo(SpaceTypeInfo typeInfo, int ordinal, String name, Class<?> type,
                                    int level) {
        assertPropertyInfo(typeInfo, ordinal, name, type, level, SpaceIndexType.NONE, null, true, true);
    }

    private void assertPropertyInfo(SpaceTypeInfo typeInfo, int ordinal, String name, Class<?> type,
                                    int level, SpaceIndexType indexType) {
        assertPropertyInfo(typeInfo, ordinal, name, type, level, indexType, null, true, true);
    }

    private void assertPropertyInfo(SpaceTypeInfo typeInfo, int ordinal, String name, Class<?> type,
                                    int level, SpaceIndexType indexType, Object nullValue) {
        assertPropertyInfo(typeInfo, ordinal, name, type, level, indexType, nullValue, true, true);
    }

    private void assertPropertyInfo(SpaceTypeInfo typeInfo, int ordinal, String name, Class<?> type,
                                    int level, SpaceIndexType indexType, Object nullValue,
                                    boolean hasGetter, boolean hasSetter) {
        SpacePropertyInfo property;
        if (ordinal < 0)
            property = typeInfo.getProperty(name);
        else {
            property = typeInfo.getProperty(ordinal);
            Assert.assertEquals(typeInfo.getProperty(name), property);
        }

        Assert.assertNotNull("property [" + name + "]", property);
        Assert.assertEquals("name", name, property.getName());
        Assert.assertEquals("type", type, property.getType());
        Assert.assertEquals("level", level, property.getLevel());
        Assert.assertEquals("null value", nullValue, property.getNullValue());

        SpaceIndex index = typeInfo.getIndexes().get(property.getName());
        if (indexType == null)
            Assert.assertNull("index type", index);
        else if (indexType == SpaceIndexType.NONE) {
            if (index != null) {
                Assert.assertEquals("index type", property.getName(), index.getName());
                Assert.assertEquals("index type", indexType, index.getIndexType());
            }
        } else {
            Assert.assertNotNull("index type", index);
            Assert.assertEquals("index type", property.getName(), index.getName());
            Assert.assertEquals("index type", indexType, index.getIndexType());
        }

        Method getter = property.getGetterMethod();
        if (hasGetter) {
            Assert.assertNotNull("getter", getter);
            Assert.assertEquals("getter parameters", 0, getter.getParameterTypes().length);
            Assert.assertEquals("getter type", property.getType(), getter.getReturnType());
        } else
            Assert.assertNull("getter", getter);

        Method setter = property.getSetterMethod();
        if (hasSetter) {
            Assert.assertNotNull("setter", setter);
            Assert.assertEquals("setter parameters", 1, setter.getParameterTypes().length);
            Assert.assertEquals("setter type", property.getType(), setter.getParameterTypes()[0]);
        } else
            Assert.assertNull("setter", setter);
    }

    public void testPojoCustomIndex() {
        // Arrange:
        final Class<?> type = PojoCustomIndex.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 3, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "str1", String.class, 0, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 1, "str2", String.class, 0, SpaceIndexType.NONE);

        Map<String, SpaceIndex> indexes = typeInfo.getIndexes();
        Assert.assertNotNull(indexes);
        Assert.assertEquals(1, indexes.size());
        testIndex(indexes, "compoundIndex", PojoCustomIndex.MyValueGetter.class);
    }

    public void testPojoCustomIndexes() {
        // Arrange:
        final Class<?> type = PojoCustomIndexes.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 3, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "str1", String.class, 0, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 1, "str2", String.class, 0, SpaceIndexType.NONE);

        Map<String, SpaceIndex> index = typeInfo.getIndexes();
        Assert.assertNotNull(index);
        Assert.assertEquals(2, index.size());
        testIndex(index, "compoundIndex", PojoCustomIndexes.MyValueGetter.class);
        testIndex(index, "compoundIndex2", PojoCustomIndexes.MyValueGetter.class);
    }

    public void testIllegalPojoCustomIndexDuplicate() {
        // Arrange:
        final Class<?> type = PojoIllegalCustomIndexes.class;

        // Act:
        try {
            SpaceTypeInfoRepository.getTypeInfo(type);
            Assert.fail("Exception should have been thrown.");

        } catch (SpaceMetadataValidationException e) {
        }
    }

    public void testPojoSpaceIndex() {
        // Arrange:
        final Class<?> type = PojoSpaceIndex.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 4, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "description", String.class, 0, SpaceIndexType.EQUAL_AND_ORDERED);
        assertPropertyInfo(typeInfo, 1, "id", int.class, 0, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 2, "personalInfo", PojoSpaceIndex.Info.class, 0, SpaceIndexType.NONE);

        Map<String, SpaceIndex> indexes = typeInfo.getIndexes();
        Assert.assertNotNull(indexes);
        Assert.assertEquals(3, indexes.size());
        testIndex(indexes, "personalInfo.name", SpaceEntryPathGetter.class, SpaceIndexType.EQUAL);
        testIndex(indexes, "id", null, SpaceIndexType.EQUAL);
        testIndex(indexes, "description", null, SpaceIndexType.EQUAL_AND_ORDERED);
    }

    public void testPojoMultipleSpaceIndex() {
        // Arrange:
        final Class<?> type = PojoMultipleSpaceIndexes.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 4, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "description", String.class, 0, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 1, "id", int.class, 0, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 2, "personalInfo", PojoMultipleSpaceIndexes.Info.class, 0, SpaceIndexType.EQUAL);

        Map<String, SpaceIndex> customIndexes = typeInfo.getIndexes();
        Assert.assertNotNull(customIndexes);
        Assert.assertEquals(4, customIndexes.size());

        testIndex(customIndexes, "personalInfo.name", SpaceEntryPathGetter.class, SpaceIndexType.EQUAL);
        testIndex(customIndexes, "personalInfo.address.zipCode", SpaceEntryPathGetter.class, SpaceIndexType.EQUAL_AND_ORDERED);
        testIndex(customIndexes, "id", null, SpaceIndexType.EQUAL);
        testIndex(customIndexes, "personalInfo", null, SpaceIndexType.EQUAL);
    }

    public void testIllegalPojoSpaceIndexDuplicate() {
        // Arrange:
        final Class<?> type = PojoIllegalDuplicateSpaceIndex.class;

        // Act:
        try {
            SpaceTypeInfoRepository.getTypeInfo(type);
            Assert.fail("Exception should have been thrown.");

        } catch (SpaceMetadataValidationException e) {
        }
    }

    public void testIllegalPojoSpaceIndexDuplicate2() {
        // Arrange:
        final Class<?> type = PojoIllegalDuplicateSpaceIndex2.class;

        // Act:
        try {
            SpaceTypeInfoRepository.getTypeInfo(type);
            Assert.fail("Exception should have been thrown.");

        } catch (SpaceMetadataValidationException e) {
        }
    }

    public void testIllegalPojoSpaceIndexDuplicate3() {
        // Arrange:
        final Class<?> type = PojoIllegalDuplicateSpaceIndex3.class;

        // Act:
        try {
            SpaceTypeInfoRepository.getTypeInfo(type);
            Assert.fail("Exception should have been thrown.");

        } catch (SpaceMetadataValidationException e) {
        }
    }

    public void testPojoXmlMultipleSpaceIndexes
            () {
        // Arrange:
        final Class<?> type = PojoSpaceIndexXml.class;

        // Act:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfProperties", 4, typeInfo.getNumOfProperties());
        assertPropertyInfo(typeInfo, "class", Class.class, true, false);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfSpaceProperties());
        assertPropertyInfo(typeInfo, 0, "description", String.class, 0, SpaceIndexType.NONE);
        assertPropertyInfo(typeInfo, 1, "id", int.class, 0, SpaceIndexType.EQUAL);
        assertPropertyInfo(typeInfo, 2, "personalInfo", PojoSpaceIndexXml.Info.class, 0, SpaceIndexType.EQUAL);

        Map<String, SpaceIndex> indexes = typeInfo.getIndexes();
        Assert.assertNotNull(indexes);
        Assert.assertEquals(5, indexes.size());

        testIndex(indexes, "id", null, SpaceIndexType.EQUAL);
        testIndex(indexes, "personalInfo", null, SpaceIndexType.EQUAL);
        testIndex(indexes, "personalInfo.id", SpaceEntryPathGetter.class, SpaceIndexType.EQUAL_AND_ORDERED);
        testIndex(indexes, "personalInfo.address.zipCode", SpaceEntryPathGetter.class, SpaceIndexType.EQUAL);
        testIndex(indexes, "personalInfo.gender", SpaceEntryPathGetter.class, SpaceIndexType.EQUAL);
    }

    public void testIllegalPojoSpaceIndexDuplicateXml() {
        // Arrange:
        final Class<?> type = PojoDuplicateSpaceIndexXml.class;

        // Act:
        try {
            SpaceTypeInfoRepository.getTypeInfo(type);
            Assert.fail("Exception should have been thrown.");

        } catch (SpaceMetadataValidationException e) {
        }
    }

    private void testIndex(Map<String, SpaceIndex> indexes, String name, Class<?> valueGetterClass) {
        final SpaceIndex index = indexes.get(name);
        Assert.assertNotNull(index);
        Assert.assertEquals(name, index.getName());
        //Assert.assertEquals(valueGetterClass, index.getIndexValueGetter().getClass());
    }

    private void testIndex(Map<String, SpaceIndex> indexes, String name, Class<?> valueGetterClass, SpaceIndexType indexType) {
        final SpaceIndex index = indexes.get(name);
        Assert.assertNotNull(index);
        Assert.assertEquals(name, index.getName());
        //Assert.assertEquals(valueGetterClass, index.getIndexValueGetter().getClass());
        Assert.assertEquals(indexType, index.getIndexType());
    }

    ///////////////////////////////////
    //       StorageType Tests       //
    ///////////////////////////////////

    public void testTypeLevelStorageType() {
        // no type level storage type declaration   
        Class<?> type = PojoPropertiesStorageType.class;
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        Assert.assertEquals(StorageType.DEFAULT, typeInfo.getStorageType());

        // Declare storage type on class
        type = PojoSpaceClassStorageType.class;
        typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        Assert.assertEquals(StorageType.COMPRESSED, typeInfo.getStorageType());

        // Declare storage type on inherit class
        type = PojoSpaceClassStorageTypeInherit.class;
        typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        Assert.assertEquals(StorageType.COMPRESSED, typeInfo.getStorageType());

        try {
            // Declare storage type on inherit class
            type = PojoSpaceClassStorageTypeInheritAndOverride.class;
            typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);
            Assert.fail("type " + type.getName() + ", expected SpaceMetadataValidationException");
        } catch (SpaceMetadataValidationException e) {
        }

        // Declare storage type on properties via gsXml
        type = PojoBasicStorageTypeXml.class;
        typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        Assert.assertEquals(StorageType.DEFAULT, typeInfo.getStorageType());

        // Declare storage type on class via gsXml
        type = PojoSpaceClassStorageTypeViaXmlDefaultType.class;
        Assert.assertEquals(StorageType.DEFAULT, SpaceTypeInfoRepository.getTypeInfo(type).getStorageType());
        type = PojoSpaceClassStorageTypeViaXmlObjectType.class;
        Assert.assertEquals(StorageType.OBJECT, SpaceTypeInfoRepository.getTypeInfo(type).getStorageType());
        type = PojoSpaceClassStorageTypeViaXmlBinaryType.class;
        Assert.assertEquals(StorageType.BINARY, SpaceTypeInfoRepository.getTypeInfo(type).getStorageType());
        type = PojoSpaceClassStorageTypeViaXmlCompressedType.class;
        Assert.assertEquals(StorageType.COMPRESSED, SpaceTypeInfoRepository.getTypeInfo(type).getStorageType());

        try {
            SpaceTypeInfoRepository.getTypeInfo(PojoIllegalSpaceClassStorageTypeXml.class);
            Assert.fail("Expected IllegalArgumentException.");
        } catch (IllegalArgumentException e) {
        }


    }


}
