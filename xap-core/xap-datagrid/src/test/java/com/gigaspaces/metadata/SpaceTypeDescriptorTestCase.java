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

import com.gigaspaces.internal.client.spaceproxy.metadata.TypeDescFactory;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
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
import com.gigaspaces.metadata.annotated.PojoIndexesInheritTrue;
import com.gigaspaces.metadata.annotated.PojoInheritenceSort1;
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
import com.gigaspaces.metadata.fifoGrouping.FifoGroupingBasicInheritPojo;
import com.gigaspaces.metadata.fifoGrouping.FifoGroupingBasicPojo;
import com.gigaspaces.metadata.fifoGrouping.FifoGroupingIllegalIndexOnCollection;
import com.gigaspaces.metadata.fifoGrouping.FifoGroupingIllegalIndexOnSpaceExclude;
import com.gigaspaces.metadata.fifoGrouping.FifoGroupingIllegalIndexWithoutMain;
import com.gigaspaces.metadata.fifoGrouping.FifoGroupingIllegalInheritPojo;
import com.gigaspaces.metadata.fifoGrouping.FifoGroupingIllegalPathInheritPojo;
import com.gigaspaces.metadata.fifoGrouping.FifoGroupingIllegalSpaceExclude;
import com.gigaspaces.metadata.fifoGrouping.FifoGroupsCollectionExceptionData;
import com.gigaspaces.metadata.fifoGrouping.FifoGroupsDoubleDefExceptionData;
import com.gigaspaces.metadata.fifoGrouping.FifoGroupsDoubleExceptionData;
import com.gigaspaces.metadata.gsxml.PojoAnnotatedAndXmled;
import com.gigaspaces.metadata.gsxml.PojoBasicStorageTypeXml;
import com.gigaspaces.metadata.gsxml.PojoDuplicateSpaceIndexXml;
import com.gigaspaces.metadata.gsxml.PojoIllegalStorageTypeXml;
import com.gigaspaces.metadata.gsxml.PojoSpaceIndexXml;
import com.gigaspaces.metadata.gsxml.fifoGrouping.PojoBasicFifoGroupingXml;
import com.gigaspaces.metadata.gsxml.fifoGrouping.PojoFifoGroupingIllegalIndexOnCollectionXml;
import com.gigaspaces.metadata.gsxml.fifoGrouping.PojoFifoGroupingInheritXml;
import com.gigaspaces.metadata.gsxml.fifoGrouping.PojoIllegalFifoGroupingDuplicatePathXml;
import com.gigaspaces.metadata.gsxml.fifoGrouping.PojoIllegalFifoGroupingDuplicateXml;
import com.gigaspaces.metadata.gsxml.fifoGrouping.PojoIllegalFifoGroupingIndexOnSpaceExcludeXml;
import com.gigaspaces.metadata.gsxml.fifoGrouping.PojoIllegalFifoGroupingIndexWithoutMainXml;
import com.gigaspaces.metadata.gsxml.fifoGrouping.PojoIllegalFifoGroupingInheritDubleDefWithPathXml;
import com.gigaspaces.metadata.gsxml.fifoGrouping.PojoIllegalFifoGroupingInheritDubleDefXml;
import com.gigaspaces.metadata.gsxml.fifoGrouping.PojoIllegalFifoGroupingOnCollectionDoubleDefXml;
import com.gigaspaces.metadata.gsxml.fifoGrouping.PojoIllegalFifoGroupingOnCollectionXml;
import com.gigaspaces.metadata.gsxml.fifoGrouping.PojoIllegalFifoGroupingPropertyOnSpaceExcludeXml;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexFactory;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.metadata.pojos.PojoIllegalStorageType;
import com.gigaspaces.metadata.pojos.PojoNoProperties;
import com.gigaspaces.metadata.pojos.PojoOneProperty;
import com.gigaspaces.metadata.pojos.PojoPropertiesStorageType;
import com.gigaspaces.metadata.pojos.PojoStorageTypeInherit;
import com.gigaspaces.metadata.pojos.PojoStorageTypeInheritAndOverride;
import com.gigaspaces.metadata.pojos.PojoStorageTypeWithOtherAnnotations;

import junit.framework.TestCase;
import org.junit.Assert;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

@com.gigaspaces.api.InternalApi
public class SpaceTypeDescriptorTestCase extends TestCase {

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
            SpaceTypeDescriptor typeInfo = new SpaceTypeDescriptorBuilder(type, null).create();
            Assert.fail("Expected IllegalArgumentNullException.");
        } catch (IllegalArgumentException e) {
        }
    }

    public void testObject() {
        // Arrange:
        final Class<?> type = Object.class;

        // Act/Assert:
        try {
            SpaceTypeDescriptor typeInfo = new SpaceTypeDescriptorBuilder(type, null).create();
            Assert.fail("Expected IllegalArgumentNullException.");
        } catch (IllegalArgumentException e) {
        }
    }

    public void testPojoNoProperties() {
        // Arrange:
        final Class<?> type = PojoNoProperties.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 0, typeInfo.getNumOfFixedProperties());
    }

    public void testPojoOneProperty() {
        // Arrange:
        final Class<?> type = PojoOneProperty.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "name", String.class);
    }

    public void testPojoCommonProperties() {
        // Arrange:
        final Class<?> type = PojoCommonProperties.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 9, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "someBoolean", boolean.class, true);
        assertPropertyInfo(typeInfo, 1, "someBooleanWrapper", Boolean.class);
        assertPropertyInfo(typeInfo, 2, "someInt", int.class, -1);
        assertPropertyInfo(typeInfo, 3, "someIntWrapper", Integer.class);
        assertPropertyInfo(typeInfo, 4, "someLong", long.class, -10L);
        assertPropertyInfo(typeInfo, 5, "someLongWrapper", Long.class);
        assertPropertyInfo(typeInfo, 6, "someObject", Object.class);
        assertPropertyInfo(typeInfo, 7, "someOverload", String.class);
        assertPropertyInfo(typeInfo, 8, "someString", String.class);
    }

    public void testPojoCommonPropertiesInheritence() {
        // Arrange:
        final Class<?> type = PojoCommonPropertiesInheritence.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 9, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "someBoolean", boolean.class, true);
        assertPropertyInfo(typeInfo, 1, "someBooleanWrapper", Boolean.class);
        assertPropertyInfo(typeInfo, 2, "someInt", int.class, -2);
        assertPropertyInfo(typeInfo, 3, "someIntWrapper", Integer.class);
        assertPropertyInfo(typeInfo, 4, "someLong", long.class, -10L);
        assertPropertyInfo(typeInfo, 5, "someLongWrapper", Long.class);
        assertPropertyInfo(typeInfo, 6, "someObject", Object.class);
        assertPropertyInfo(typeInfo, 7, "someOverload", String.class);
        assertPropertyInfo(typeInfo, 8, "someString", String.class);
    }

    public void testPojoInheritenceSort1() {
        // Arrange:
        final Class<?> type = PojoInheritenceSort1.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 7, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "fieldB", String.class);
        assertPropertyInfo(typeInfo, 1, "fieldD", String.class);
        assertPropertyInfo(typeInfo, 2, "fieldD1", String.class);
        assertPropertyInfo(typeInfo, 3, "fieldD2", String.class);
        assertPropertyInfo(typeInfo, 4, "fieldD3", String.class);
        assertPropertyInfo(typeInfo, 5, "fieldD4", String.class);
        assertPropertyInfo(typeInfo, 6, "fieldD6", String.class);

        assertNumOfIndexes(typeInfo, 1);
        assertIndex(typeInfo, "fieldD6", SpaceIndexType.EQUAL);
    }

    /*
    public void testPojoInheritenceSort2()
    {
        // Arrange:
        final Class<?> type = PojoInheritenceSort2.class;
        
        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 13, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "fieldB", String.class);
        assertPropertyInfo(typeInfo, 1, "fieldD", String.class);
        assertPropertyInfo(typeInfo, 2, "fieldD1", String.class);
        assertPropertyInfo(typeInfo, 3, "fieldD2", String.class);
        assertPropertyInfo(typeInfo, 4, "fieldD3", String.class);
        assertPropertyInfo(typeInfo, 5, "fieldD4", String.class);
        assertPropertyInfo(typeInfo, 6, "fieldD6", String.class);
        assertPropertyInfo(typeInfo, 7, "fieldA", String.class);
        assertPropertyInfo(typeInfo, 8, "fieldB1", String.class);
        assertPropertyInfo(typeInfo, 9, "fieldB2", String.class);
        assertPropertyInfo(typeInfo, 10, "fieldC", String.class);
        assertPropertyInfo(typeInfo, 11, "fieldD5", String.class);
        assertPropertyInfo(typeInfo, 12, "fieldE", String.class);

        assertNumOfIndexes(typeInfo, 1);
        assertIndex(typeInfo, "fieldD4", SpaceIndexType.EQUAL);
    }        
    */
    public void testPojoImplicit() {
        // Arrange:
        final Class<?> type = PojoImplicit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class);
        assertPropertyInfo(typeInfo, 1, "implicitProperty", String.class);
    }

    public void testPojoImplicitExDefault() {
        // Arrange:
        final Class<?> type = PojoImplicitExDefault.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class);
        assertPropertyInfo(typeInfo, 1, "implicitProperty", String.class);
    }

    public void testPojoImplicitExEcplicit() {
        // Arrange:
        final Class<?> type = PojoImplicitExExplicit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class);
        assertPropertyInfo(typeInfo, 1, "implicitProperty", String.class);
    }

    public void testPojoImplicitExImplicit() {
        // Arrange:
        final Class<?> type = PojoImplicitExImplicit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class);
        assertPropertyInfo(typeInfo, 1, "implicitProperty", String.class);
    }

    public void testPojoExplicit() {
        // Arrange:
        final Class<?> type = PojoExplicit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class);
        assertPropertyInfo(typeInfo, 1, "pojoId", Integer.class);
        assertPropertyInfo(typeInfo, 2, "pojoIndexedField", Integer.class);

        assertNumOfIndexes(typeInfo, 2);
        assertIndex(typeInfo, "pojoId", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "pojoIndexedField", SpaceIndexType.EQUAL);
    }

    public void testPojoExplicitExDefault() {
        // Arrange:
        final Class<?> type = PojoExplicitExDefault.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class);
        assertPropertyInfo(typeInfo, 1, "pojoId", Integer.class);
        assertPropertyInfo(typeInfo, 2, "pojoIndexedField", Integer.class);
        assertNumOfIndexes(typeInfo, 2);
        assertIndex(typeInfo, "pojoId", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "pojoIndexedField", SpaceIndexType.EQUAL);
    }

    public void testPojoExplicitExExplicit() {
        // Arrange:
        final Class<?> type = PojoExplicitExExplicit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class);
        assertPropertyInfo(typeInfo, 1, "pojoId", Integer.class);
        assertPropertyInfo(typeInfo, 2, "pojoIndexedField", Integer.class);
        assertNumOfIndexes(typeInfo, 2);
        assertIndex(typeInfo, "pojoId", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "pojoIndexedField", SpaceIndexType.EQUAL);
    }

    public void testPojoExplicitExImplicit() {
        // Arrange:
        final Class<?> type = PojoExplicitExImplicit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "explicitProperty", String.class);
        assertPropertyInfo(typeInfo, 1, "pojoId", Integer.class);
        assertPropertyInfo(typeInfo, 2, "pojoIndexedField", Integer.class);
        assertNumOfIndexes(typeInfo, 2);
        assertIndex(typeInfo, "pojoId", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "pojoIndexedField", SpaceIndexType.EQUAL);
    }

    public void testPojoAttributes() {
        // Arrange:
        final Class<?> type = PojoAttributes.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class);
        assertPropertyInfo(typeInfo, 1, "routing", String.class);
        assertNumOfIndexes(typeInfo, 2);
        assertIndex(typeInfo, "id", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "routing", SpaceIndexType.EQUAL);
        assertIdProperty(typeInfo, "id", false);
        //Assert.assertEquals("leaseExpiration", typeInfo.getProperty("leaseExpiration"), typeInfo.getLeaseExpirationProperty());
        //Assert.assertEquals("persist", typeInfo.getProperty("persist"), typeInfo.getPersistProperty());
        //Assert.assertEquals("routing", typeInfo.getProperty("routing"), typeInfo.getRoutingProperty());
        //Assert.assertEquals("version", typeInfo.getProperty("version"), typeInfo.getVersionProperty());
    }

    public void testPojoAttributesInheritence1() {
        // Arrange:
        final Class<?> type = PojoAttributesInheritence1.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class);
        assertPropertyInfo(typeInfo, 1, "routing", String.class);
        assertNumOfIndexes(typeInfo, 2);
        assertIndex(typeInfo, "id", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "routing", SpaceIndexType.EQUAL);

        assertIdProperty(typeInfo, "id", false);
        //Assert.assertEquals("leaseExpiration", typeInfo.getProperty("leaseExpiration"), typeInfo.getLeaseExpirationProperty());
        //Assert.assertEquals("persist", typeInfo.getProperty("persist"), typeInfo.getPersistProperty());
        //Assert.assertEquals("routing", typeInfo.getProperty("routing"), typeInfo.getRoutingProperty());
        //Assert.assertEquals("version", typeInfo.getProperty("version"), typeInfo.getVersionProperty());
    }

    public void testPojoAttributesInheritence2() {
        // Arrange:
        final Class<?> type = PojoAttributesInheritence2.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 4, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class);
        assertPropertyInfo(typeInfo, 1, "routing", String.class);
        assertPropertyInfo(typeInfo, 2, "id2", String.class);
        assertPropertyInfo(typeInfo, 3, "routing2", String.class);
        assertNumOfIndexes(typeInfo, 4);
        assertIndex(typeInfo, "id", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "routing", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "id2", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "routing2", SpaceIndexType.EQUAL);

        assertIdProperty(typeInfo, "id2", false);
        //Assert.assertEquals("leaseExpiration", typeInfo.getProperty("leaseExpiration2"), typeInfo.getLeaseExpirationProperty());
        //Assert.assertEquals("persist", typeInfo.getProperty("persist2"), typeInfo.getPersistProperty());
        //Assert.assertEquals("routing", typeInfo.getProperty("routing2"), typeInfo.getRoutingProperty());
        //Assert.assertEquals("version", typeInfo.getProperty("version2"), typeInfo.getVersionProperty());
    }

    public void testPojoRoutingIndexImplicit() {
        // Arrange:
        final Class<?> type = PojoRoutingIndexImplicit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "routing", String.class);
        assertNumOfIndexes(typeInfo, 1);
        assertIndex(typeInfo, "routing", SpaceIndexType.EQUAL);
    }

    public void testPojoRoutingIndexExplicit() {
        // Arrange:
        final Class<?> type = PojoRoutingIndexExplicit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "routing", String.class);
        assertNumOfIndexes(typeInfo, 0);
    }

    public void testPojoIdIndexImplicit() {
        // Arrange:
        final Class<?> type = PojoIdIndexImplicit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class);
        assertNumOfIndexes(typeInfo, 1);
        assertIndex(typeInfo, "id", SpaceIndexType.EQUAL);
        assertIdProperty(typeInfo, "id", false);
    }

    public void testPojoIdIndexExplicit() {
        // Arrange:
        final Class<?> type = PojoIdIndexExplicit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class);
        assertNumOfIndexes(typeInfo, 0);
    }

    public void testPojoIdAutogenerateIndexImplicit() {
        // Arrange:
        final Class<?> type = PojoIdAutogenerateIndexImplicit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class);
        assertNumOfIndexes(typeInfo, 0);
        assertIdProperty(typeInfo, "id", true);
    }

    public void testPojoIdAutogenerateIndexExplicit() {
        // Arrange:
        final Class<?> type = PojoIdAutogenerateIndexExplicit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class);
        assertNumOfIndexes(typeInfo, 1);
        assertIndex(typeInfo, "id", SpaceIndexType.EQUAL);
        assertIdProperty(typeInfo, "id", true);
    }

    public void testPojoIdAutogenerateIndexImplicitInherit() {
        // Arrange:
        final Class<?> type = PojoIdAutogenerateIndexImplicitInherit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 1, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "id", String.class);
        assertNumOfIndexes(typeInfo, 0);
        assertIdProperty(typeInfo, "id", true);
    }

    public void testPojoIndexes() {
        // Arrange:
        final Class<?> type = PojoIndexes.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 4, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "valueBasic", String.class);
        assertPropertyInfo(typeInfo, 1, "valueDefault", String.class);
        assertPropertyInfo(typeInfo, 2, "valueExtended", String.class);
        assertPropertyInfo(typeInfo, 3, "valueNone", String.class);
        assertNumOfIndexes(typeInfo, 2);
        assertIndex(typeInfo, "valueBasic", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "valueExtended", SpaceIndexType.EQUAL_AND_ORDERED);
    }

    public void testPojoIndexesInheritTrue() {
        // Arrange:
        final Class<?> type = PojoIndexesInheritTrue.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 8, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "valueBasic", String.class);
        assertPropertyInfo(typeInfo, 1, "valueDefault", String.class);
        assertPropertyInfo(typeInfo, 2, "valueExtended", String.class);
        assertPropertyInfo(typeInfo, 3, "valueNone", String.class);
        assertPropertyInfo(typeInfo, 4, "valueBasic2", String.class);
        assertPropertyInfo(typeInfo, 5, "valueDefault2", String.class);
        assertPropertyInfo(typeInfo, 6, "valueExtended2", String.class);
        assertPropertyInfo(typeInfo, 7, "valueNone2", String.class);
        assertNumOfIndexes(typeInfo, 4);
        assertIndex(typeInfo, "valueBasic", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "valueExtended", SpaceIndexType.EQUAL_AND_ORDERED);
        assertIndex(typeInfo, "valueBasic2", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "valueExtended2", SpaceIndexType.EQUAL_AND_ORDERED);
    }

    /*
        public void testPojoIndexesInheritFalse()
        {
            // Arrange:
            final Class<?> type = PojoIndexesInheritFalse.class;

            // Act:
            SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

            // Assert:
            assertSpaceTypeInfo(typeInfo, type);
            Assert.assertEquals("numOfSpaceProperties", 8, typeInfo.getNumOfFixedProperties());
            assertPropertyInfo(typeInfo, 0, "valueBasic", String.class);
            assertPropertyInfo(typeInfo, 1, "valueDefault", String.class);
            assertPropertyInfo(typeInfo, 2, "valueExtended", String.class);
            assertPropertyInfo(typeInfo, 3, "valueNone", String.class);
            assertPropertyInfo(typeInfo, 4, "valueBasic2", String.class);
            assertPropertyInfo(typeInfo, 5, "valueDefault2", String.class);
            assertPropertyInfo(typeInfo, 6, "valueExtended2", String.class);
            assertPropertyInfo(typeInfo, 7, "valueNone2", String.class);
            assertNumOfIndexes(typeInfo, 2);
            assertIndex(typeInfo, "valueBasic2", SpaceIndexType.EQUAL);
            assertIndex(typeInfo, "valueExtended2", SpaceIndexType.EQUAL_AND_ORDERED);
        }
    */
    public void testPojoPojoAnnotatedAndXmled() {
        // Arrange:
        final Class<?> type = PojoAnnotatedAndXmled.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "someProperty1", String.class);
        assertPropertyInfo(typeInfo, 1, "someProperty2", String.class);
        assertPropertyInfo(typeInfo, 2, "someProperty3", String.class);
        assertIdProperty(typeInfo, "someProperty2", true);
        assertNumOfIndexes(typeInfo, 1);
        assertIndex(typeInfo, "someProperty3", SpaceIndexType.EQUAL);
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
                SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);
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
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "value", int.class, 0);
        assertNumOfIndexes(typeInfo, 1);
        assertIndex(typeInfo, "value", SpaceIndexType.EQUAL);
    }

    public void testInheritOverrideNullValue() {
        // Arrange:
        final Class<?> type = PojoSpacePropertyInherit.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 1, "value2", int.class, -1);
        assertNumOfIndexes(typeInfo, 1);
        assertIndex(typeInfo, "value", SpaceIndexType.EQUAL);
    }

    private void assertSpaceTypeInfo(SpaceTypeDescriptor typeInfo, Class<?> type) {
        Assert.assertEquals("getObjectClass", type, typeInfo.getObjectClass());
        Assert.assertEquals("getTypeName", type.getName(), typeInfo.getTypeName());
        
        /* TODO: Restore this test if/when super type info is available.
        Class<?> superType = type.getSuperclass();
        if (superType == null)
            Assert.assertEquals(null, typeInfo.getSuperTypeInfo());
        else
            assertSpaceTypeInfo(typeInfo.getSuperTypeInfo(), superType);
        */
    }

    private void assertPropertyInfo(SpaceTypeDescriptor typeInfo, int ordinal, String name, Class<?> type) {
        assertPropertyInfo(typeInfo, ordinal, name, type, null);
    }

    private void assertPropertyInfo(SpaceTypeDescriptor typeInfo, int ordinal, String name, Class<?> type, Object nullValue) {
        SpacePropertyDescriptor property;
        if (ordinal < 0)
            property = typeInfo.getFixedProperty(name);
        else {
            property = typeInfo.getFixedProperty(ordinal);
            Assert.assertEquals(typeInfo.getFixedProperty(name), property);
        }

        Assert.assertNotNull("property [" + name + "]", property);
        Assert.assertEquals("name", name, property.getName());
        Assert.assertEquals("type", type, property.getType());
        //Assert.assertEquals("null value", nullValue, property.getNullValue());

        /*
        Method getter = property.getGetterMethod();
        if (hasGetter)
        {
            Assert.assertNotNull("getter", getter);
            Assert.assertEquals("getter parameters", 0, getter.getParameterTypes().length);
            Assert.assertEquals("getter type", property.getType(), getter.getReturnType());
        }
        else
            Assert.assertNull("getter", getter);

        Method setter = property.getSetterMethod();
        if (hasSetter)
        {
            Assert.assertNotNull("setter", setter);
            Assert.assertEquals("setter parameters", 1, setter.getParameterTypes().length);
            Assert.assertEquals("setter type", property.getType(), setter.getParameterTypes()[0]);
        }
        else
            Assert.assertNull("setter", setter);
        */
    }

    private void assertNumOfIndexes(SpaceTypeDescriptor typeInfo, int expectedNumOfIndexes) {

        int numOfIndexes = 0;
        for (SpaceIndex index : typeInfo.getIndexes().values())
            if (index.getIndexType().isIndexed())
                numOfIndexes++;
        Assert.assertEquals(expectedNumOfIndexes, numOfIndexes);

        //Assert.assertEquals(expectedNumOfIndexes, typeInfo.getIndexes().size());
    }

    private void assertIndex(SpaceTypeDescriptor typeInfo, String name, SpaceIndexType indexType) {
        assertIndex(typeInfo, name, indexType, null);
    }

    private void assertIndex(SpaceTypeDescriptor typeInfo, String name, SpaceIndexType indexType, Class<?> valueGetterClass) {
        SpaceIndex spaceIndex = typeInfo.getIndexes().get(name);
        Assert.assertNotNull(spaceIndex);
        Assert.assertEquals(name, spaceIndex.getName());
        Assert.assertEquals(indexType, spaceIndex.getIndexType());
        // TODO: Assert valueGetterClass
    }

    private static void assertIdProperty(SpaceTypeDescriptor typeInfo, String propertyName, boolean isAutoGenerate) {
        Assert.assertEquals(propertyName, typeInfo.getIdPropertyName());
        Assert.assertEquals(isAutoGenerate, getInternalTypeDesc(typeInfo).isAutoGenerateId());
    }

    private static void assertRoutingProperty(SpaceTypeDescriptor typeInfo, String propertyName) {
        Assert.assertEquals(propertyName, typeInfo.getRoutingPropertyName());
    }


    public void testPojoCustomIndex() {
        // Arrange:
        final Class<?> type = PojoCustomIndex.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "str1", String.class);
        assertPropertyInfo(typeInfo, 1, "str2", String.class);
        assertNumOfIndexes(typeInfo, 1);
        assertIndex(typeInfo, "compoundIndex", SpaceIndexType.EQUAL, PojoCustomIndex.MyValueGetter.class);
    }

    public void testPojoCustomIndexes() {
        // Arrange:
        final Class<?> type = PojoCustomIndexes.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 2, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "str1", String.class);
        assertPropertyInfo(typeInfo, 1, "str2", String.class);
        assertNumOfIndexes(typeInfo, 2);
        assertIndex(typeInfo, "compoundIndex", SpaceIndexType.EQUAL, PojoCustomIndexes.MyValueGetter.class);
        assertIndex(typeInfo, "compoundIndex2", SpaceIndexType.EQUAL, PojoCustomIndexes.MyValueGetter.class);
    }

    public void testIllegalPojoCustomIndexDuplicate() {
        // Arrange:
        final Class<?> type = PojoIllegalCustomIndexes.class;

        // Act:
        try {
            SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);
            Assert.fail("Exception should have been thrown.");

        } catch (SpaceMetadataValidationException e) {
        }
    }

    public void testPojoSpaceIndex() {
        // Arrange:
        final Class<?> type = PojoSpaceIndex.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "description", String.class);
        assertPropertyInfo(typeInfo, 1, "id", int.class);
        assertPropertyInfo(typeInfo, 2, "personalInfo", PojoSpaceIndex.Info.class);
        assertNumOfIndexes(typeInfo, 3);
        assertIndex(typeInfo, "description", SpaceIndexType.EQUAL_AND_ORDERED);
        assertIndex(typeInfo, "id", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "personalInfo.name", SpaceIndexType.EQUAL);
    }

    public void testPojoMultipleSpaceIndex() {
        // Arrange:
        final Class<?> type = PojoMultipleSpaceIndexes.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "description", String.class);
        assertPropertyInfo(typeInfo, 1, "id", int.class);
        assertPropertyInfo(typeInfo, 2, "personalInfo", PojoMultipleSpaceIndexes.Info.class);
        assertNumOfIndexes(typeInfo, 4);
        assertIndex(typeInfo, "id", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "personalInfo", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "personalInfo.name", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "personalInfo.address.zipCode", SpaceIndexType.EQUAL_AND_ORDERED);
    }

    public void testIllegalPojoSpaceIndexDuplicate() {
        // Arrange:
        final Class<?> type = PojoIllegalDuplicateSpaceIndex.class;

        // Act:
        try {
            SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);
            Assert.fail("Exception should have been thrown.");

        } catch (SpaceMetadataValidationException e) {
        }
    }

    public void testIllegalPojoSpaceIndexDuplicate2() {
        // Arrange:
        final Class<?> type = PojoIllegalDuplicateSpaceIndex2.class;

        // Act:
        try {
            SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);
            Assert.fail("Exception should have been thrown.");

        } catch (SpaceMetadataValidationException e) {
        }
    }

    public void testIllegalPojoSpaceIndexDuplicate3() {
        // Arrange:
        final Class<?> type = PojoIllegalDuplicateSpaceIndex3.class;

        // Act:
        try {
            SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);
            Assert.fail("Exception should have been thrown.");

        } catch (SpaceMetadataValidationException e) {
        }
    }

    public void testPojoXmlMultipleSpaceIndexes
            () {
        // Arrange:
        final Class<?> type = PojoSpaceIndexXml.class;

        // Act:
        SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);

        // Assert:
        assertSpaceTypeInfo(typeInfo, type);
        Assert.assertEquals("numOfSpaceProperties", 3, typeInfo.getNumOfFixedProperties());
        assertPropertyInfo(typeInfo, 0, "description", String.class);
        assertPropertyInfo(typeInfo, 1, "id", int.class);
        assertPropertyInfo(typeInfo, 2, "personalInfo", PojoSpaceIndexXml.Info.class);
        assertNumOfIndexes(typeInfo, 5);
        assertIndex(typeInfo, "id", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "personalInfo", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "personalInfo.id", SpaceIndexType.EQUAL_AND_ORDERED);
        assertIndex(typeInfo, "personalInfo.address.zipCode", SpaceIndexType.EQUAL);
        assertIndex(typeInfo, "personalInfo.gender", SpaceIndexType.EQUAL);
    }

    public void testIllegalPojoSpaceIndexDuplicateXml() {
        // Arrange:
        final Class<?> type = PojoDuplicateSpaceIndexXml.class;

        // Act:
        try {
            SpaceTypeDescriptor typeInfo = createSpaceTypeDesc(type);
            Assert.fail("Exception should have been thrown.");

        } catch (SpaceMetadataValidationException e) {
        }
    }

    public void testImplicitIdProperty() {
        // Test implicit id is created when no id is set:
        SpaceTypeDescriptor typeDesc = new SpaceTypeDescriptorBuilder("foo").create();
        Assert.assertEquals("numOfSpaceProperties", 1, typeDesc.getNumOfFixedProperties());
        assertPropertyInfo(typeDesc, 0, "_spaceId", String.class);
        assertIdProperty(typeDesc, "_spaceId", true);
        assertRoutingProperty(typeDesc, "_spaceId");
        assertNumOfIndexes(typeDesc, 0);

        // Test implicit id is not created when id is set:
        typeDesc = new SpaceTypeDescriptorBuilder("foo").idProperty("myId").create();
        Assert.assertEquals("numOfSpaceProperties", 1, typeDesc.getNumOfFixedProperties());
        assertPropertyInfo(typeDesc, 0, "myId", Object.class);
        assertIdProperty(typeDesc, "myId", false);
        assertRoutingProperty(typeDesc, "myId");
        assertNumOfIndexes(typeDesc, 1);

        // Test implicit id is not created when id is set at super type:
        typeDesc = new SpaceTypeDescriptorBuilder("foo", typeDesc).create();
        Assert.assertEquals("numOfSpaceProperties", 1, typeDesc.getNumOfFixedProperties());
        assertPropertyInfo(typeDesc, 0, "myId", Object.class);
        assertIdProperty(typeDesc, "myId", false);
        assertRoutingProperty(typeDesc, "myId");
        assertNumOfIndexes(typeDesc, 1);
    }

    private static SpaceTypeDescriptor createSpaceTypeDesc(Class<?> type) {
        final Class<?> superType = type.getSuperclass();
        final SpaceTypeDescriptor superTypeInfo = (superType == null || superType.equals(Object.class)) ? null : createSpaceTypeDesc(superType);
        SpaceTypeDescriptor typeInfo = new SpaceTypeDescriptorBuilder(type, superTypeInfo).create();
        return typeInfo;
    }

    private static ITypeDesc getInternalTypeDesc(SpaceTypeDescriptor typeInfo) {
        return (ITypeDesc) typeInfo;
    }

    ///////////////////////////////////
    //       StorageType Tests       //
    ///////////////////////////////////

    public void testPojoStorageType() {
        // Arrange:
        ITypeDesc typeDesc = new TypeDescFactory().createPojoTypeDesc(PojoPropertiesStorageType.class, null, null);
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(PojoPropertiesStorageType.class);
        // Assert:
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("id").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("routing").getStorageType());

        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("someInt").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("someLong").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("someBoolean").getStorageType());

        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_objectStorageTypeObject").getStorageType());
        Assert.assertEquals(StorageType.BINARY, typeDesc.getFixedProperty("_binaryStorageTypeList").getStorageType());
        Assert.assertEquals(StorageType.COMPRESSED, typeDesc.getFixedProperty("_compressedStorageTypeMap").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_defaultStorageTypeArrayList").getStorageType());

        Assert.assertNull("spaceExcludeObject storageType should have been null", typeInfo.getProperty("spaceExcludeObject").getStorageType());
        Assert.assertNull("dynamicPropertiesMap storageType should have been null", typeInfo.getProperty("dynamicPropertiesMap").getStorageType());
        Assert.assertNull("persistent storageType should have been null", typeInfo.getProperty("persistent").getStorageType());
        Assert.assertNull("version storageType should have been null", typeInfo.getProperty("version").getStorageType());
        Assert.assertNull("leasExpiration storageType should have been null", typeInfo.getProperty("leasExpiration").getStorageType());

        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("basicIndexObject").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("extendedIndexObject").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("noneIndexObject").getStorageType());
    }

    public void testStorageTypeWithOtherAnnotations() {
        // Arrange:
        final Class<?> type = PojoStorageTypeWithOtherAnnotations.class;
        ITypeDesc typeDesc = new TypeDescFactory().createPojoTypeDesc(type, null, null);
        // Assert:
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("id").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("routing").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("object1").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("object2").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("object3").getStorageType());
        Assert.assertEquals(StorageType.BINARY, typeDesc.getFixedProperty("binary1").getStorageType());
        Assert.assertEquals(StorageType.BINARY, typeDesc.getFixedProperty("binary2").getStorageType());
        Assert.assertEquals(StorageType.COMPRESSED, typeDesc.getFixedProperty("compressed1").getStorageType());
        Assert.assertEquals(StorageType.COMPRESSED, typeDesc.getFixedProperty("compressed2").getStorageType());
    }

    public void testPojoStorageTypeInherit() {
        // Arrange:
        final Class<?> type = PojoStorageTypeInherit.class;
        ITypeDesc typeDesc = new TypeDescFactory().createPojoTypeDesc(type, null, null);
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("id").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("routing").getStorageType());

        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("someInt").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("someLong").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("someBoolean").getStorageType());

        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_objectStorageTypeObject").getStorageType());
        Assert.assertEquals(StorageType.BINARY, typeDesc.getFixedProperty("_binaryStorageTypeList").getStorageType());
        Assert.assertEquals(StorageType.COMPRESSED, typeDesc.getFixedProperty("_compressedStorageTypeMap").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_defaultStorageTypeArrayList").getStorageType());

        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("basicIndexObject").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("extendedIndexObject").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("noneIndexObject").getStorageType());

        Assert.assertNull("spaceExcludeObject storageType should have been null", typeInfo.getProperty("spaceExcludeObject").getStorageType());
        Assert.assertNull("dynamicPropertiesMap storageType should have been null", typeInfo.getProperty("dynamicPropertiesMap").getStorageType());
        Assert.assertNull("persistent storageType should have been null", typeInfo.getProperty("persistent").getStorageType());
        Assert.assertNull("version storageType should have been null", typeInfo.getProperty("version").getStorageType());
        Assert.assertNull("leasExpiration storageType should have been null", typeInfo.getProperty("leasExpiration").getStorageType());
    }

    public void testStorageTypeInheritAndOverride() {
        // Arrange:
        final Class<?> type = PojoStorageTypeInheritAndOverride.class;
        ITypeDesc typeDesc = new TypeDescFactory().createPojoTypeDesc(type, null, null);
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Assert:
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("id").getStorageType()); // override. no storageType annotation declared.
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("routing").getStorageType());

        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("someInt").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("someLong").getStorageType()); // override and declare (the same) StorageType.OBJECT
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("someBoolean").getStorageType());

        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_objectStorageTypeObject").getStorageType());
        Assert.assertEquals(StorageType.BINARY, typeDesc.getFixedProperty("_binaryStorageTypeList").getStorageType()); // override and declare (the same) StorageType.BINARY
        Assert.assertEquals(StorageType.COMPRESSED, typeDesc.getFixedProperty("_compressedStorageTypeMap").getStorageType()); // override and trying to change to default (remain COMPRESSED)
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_defaultStorageTypeArrayList").getStorageType()); // override. no storageType annotation declared.

        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("basicIndexObject").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("extendedIndexObject").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("noneIndexObject").getStorageType()); //  override and declare (the same) StorageType.DEFAULT

        Assert.assertNull("spaceExcludeObject storageType should have been null", typeInfo.getProperty("spaceExcludeObject").getStorageType());
        Assert.assertNull("dynamicPropertiesMap storageType should have been null", typeInfo.getProperty("dynamicPropertiesMap").getStorageType());
        Assert.assertNull("persistent storageType should have been null", typeInfo.getProperty("persistent").getStorageType());
        Assert.assertNull("version storageType should have been null", typeInfo.getProperty("version").getStorageType());
        Assert.assertNull("leasExpiration storageType should have been null", typeInfo.getProperty("leasExpiration").getStorageType());
    }

    public void testPojoStorageTypeXml() {
        // Arrange:
        final Class<?> type = PojoBasicStorageTypeXml.class;
        ITypeDesc typeDesc = new TypeDescFactory().createPojoTypeDesc(type, null, null);
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);


        // validate storage types
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_id").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_routing").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_explicitDefaultStorageTypeOnPrimitive").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_explicitObjectStorageTypeOnPrimitive").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_noStorageTypePrimitive").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_explicitDefaultStorageTypeObject").getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("_objectStorageTypeObject").getStorageType());
        Assert.assertEquals(StorageType.BINARY, typeDesc.getFixedProperty("_binaryStorageTypeObject").getStorageType());
        Assert.assertEquals(StorageType.COMPRESSED, typeDesc.getFixedProperty("_compressedStorageTypeObject").getStorageType());
    }

    public void testStorageTypeDocument() {
        String typeName = "StorageTypeDocument";
        SpaceTypeDescriptorBuilder docBuilder =
                BuildBasicDocument(typeName, null)
                        .storageType(StorageType.BINARY)
                        .addFixedProperty("storageTypeProperty", Object.class, StorageType.COMPRESSED);
        ITypeDesc typeDesc = (ITypeDesc) docBuilder.create();

        Assert.assertEquals(StorageType.BINARY, typeDesc.getStorageType());
        Assert.assertEquals(StorageType.OBJECT, typeDesc.getFixedProperty("id").getStorageType());
        Assert.assertEquals(StorageType.BINARY, typeDesc.getFixedProperty("info").getStorageType());
        Assert.assertEquals(StorageType.COMPRESSED, typeDesc.getFixedProperty("storageTypeProperty").getStorageType());

        String inheritTypeName = "InheritStorageTypeDocument";
        SpaceTypeDescriptorBuilder inheritDocBuilder =
                BuildBasicDocument(inheritTypeName, typeDesc);
        ITypeDesc inheritTypeDesc = (ITypeDesc) inheritDocBuilder.create();

        Assert.assertEquals(StorageType.BINARY, inheritTypeDesc.getStorageType());
        Assert.assertEquals(StorageType.COMPRESSED, inheritTypeDesc.getFixedProperty("storageTypeProperty").getStorageType());
    }

    public void testIllegalStorageType() {
        nonOBJECTStorageTypeOnPrimitiveProperty();

        declareStorageTypeOnInheritedStorageType();

        declareStorageTypeWithOtherAnnotation();
        // as long as storage type cannot be overridden an exception is thrown and 
        // the following test passes regardless of illegal annotation combinations.
        declareStorageTypeOnInheritedAnnotatedProperty();
        inheritStorageTypeAndDeclareAnnotation();

        IllegalStorageTypeDocument();
    }

    public void testExplicitRoutingIndex() {
        // Test implicit routing index:
        testExplicitRoutingIndex(SpaceIndexType.EQUAL, false);
        testExplicitRoutingIndex(SpaceIndexType.BASIC, false);
        testExplicitRoutingIndex(SpaceIndexType.ORDERED, true);
        testExplicitRoutingIndex(SpaceIndexType.EQUAL_AND_ORDERED, true);
        testExplicitRoutingIndex(SpaceIndexType.EXTENDED, true);

        // Test explicit new default routing index:
        testExplicitRoutingIndex(SpaceIndexType.EQUAL, SpaceIndexType.EQUAL, false);
        testExplicitRoutingIndex(SpaceIndexType.EQUAL, SpaceIndexType.BASIC, false);
        testExplicitRoutingIndex(SpaceIndexType.EQUAL, SpaceIndexType.ORDERED, true);
        testExplicitRoutingIndex(SpaceIndexType.EQUAL, SpaceIndexType.EQUAL_AND_ORDERED, true);
        testExplicitRoutingIndex(SpaceIndexType.EQUAL, SpaceIndexType.EXTENDED, true);

        // Test explicit old default routing index:
        testExplicitRoutingIndex(SpaceIndexType.BASIC, SpaceIndexType.EQUAL, false);
        testExplicitRoutingIndex(SpaceIndexType.BASIC, SpaceIndexType.BASIC, false);
        testExplicitRoutingIndex(SpaceIndexType.BASIC, SpaceIndexType.ORDERED, true);
        testExplicitRoutingIndex(SpaceIndexType.BASIC, SpaceIndexType.EQUAL_AND_ORDERED, true);
        testExplicitRoutingIndex(SpaceIndexType.BASIC, SpaceIndexType.EXTENDED, true);

        // Test explicit non-default routing index:
        testExplicitRoutingIndex(SpaceIndexType.ORDERED, SpaceIndexType.EQUAL, true);
        testExplicitRoutingIndex(SpaceIndexType.ORDERED, SpaceIndexType.BASIC, true);
        testExplicitRoutingIndex(SpaceIndexType.ORDERED, SpaceIndexType.ORDERED, false);
        testExplicitRoutingIndex(SpaceIndexType.ORDERED, SpaceIndexType.EQUAL_AND_ORDERED, true);
        testExplicitRoutingIndex(SpaceIndexType.ORDERED, SpaceIndexType.EXTENDED, true);

        // Test explicit non-default routing index:
        testExplicitRoutingIndex(SpaceIndexType.EQUAL_AND_ORDERED, SpaceIndexType.EQUAL, true);
        testExplicitRoutingIndex(SpaceIndexType.EQUAL_AND_ORDERED, SpaceIndexType.BASIC, true);
        testExplicitRoutingIndex(SpaceIndexType.EQUAL_AND_ORDERED, SpaceIndexType.ORDERED, true);
        testExplicitRoutingIndex(SpaceIndexType.EQUAL_AND_ORDERED, SpaceIndexType.EQUAL_AND_ORDERED, false);
        testExplicitRoutingIndex(SpaceIndexType.EQUAL_AND_ORDERED, SpaceIndexType.EXTENDED, false);

        // Test explicit non-default routing index:
        testExplicitRoutingIndex(SpaceIndexType.EXTENDED, SpaceIndexType.EQUAL, true);
        testExplicitRoutingIndex(SpaceIndexType.EXTENDED, SpaceIndexType.BASIC, true);
        testExplicitRoutingIndex(SpaceIndexType.EXTENDED, SpaceIndexType.ORDERED, true);
        testExplicitRoutingIndex(SpaceIndexType.EXTENDED, SpaceIndexType.EQUAL_AND_ORDERED, false);
        testExplicitRoutingIndex(SpaceIndexType.EXTENDED, SpaceIndexType.EXTENDED, false);
    }

    private void testExplicitRoutingIndex(SpaceIndexType propIndexType, boolean shouldFail) {
        testExplicitRoutingIndex(null, propIndexType, shouldFail);
    }
    private void testExplicitRoutingIndex(SpaceIndexType routingIndexType, SpaceIndexType propIndexType, boolean shouldFail) {
        SpaceTypeDescriptorBuilder builder = new SpaceTypeDescriptorBuilder("foo");
        builder.addIndex(SpaceIndexFactory.createPropertyIndex("property", propIndexType));

        try {
            if (routingIndexType == null)
                builder.routingProperty("property");
            else
                builder.routingProperty("property", routingIndexType);
            if (shouldFail)
                Assert.fail("Should have failed");
        } catch (RuntimeException e) {
            if (!shouldFail)
                throw e;
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains(propIndexType.name()));
            return;
        }
    }

    private void nonOBJECTStorageTypeOnPrimitiveProperty() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.OnIntegerTypeProperty.class,
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.OnStringTypeProperty.class,
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.OnBooleanTypeProperty.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.OnIntegerTypeProperty.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.OnStringTypeProperty.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.OnBooleanTypeProperty.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void declareStorageTypeOnInheritedStorageType() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.InheritStorageType.ObjectType.DeclareStorageType.BinaryType.class,
                        PojoIllegalStorageType.InheritStorageType.ObjectType.DeclareStorageType.CompressedType.class,
                        PojoIllegalStorageType.InheritStorageType.BinaryType.DeclareStorageType.ObjectType.class,
                        PojoIllegalStorageType.InheritStorageType.BinaryType.DeclareStorageType.CompressedType.class,
                        PojoIllegalStorageType.InheritStorageType.CompressedType.DeclareStorageType.ObjectType.class,
                        PojoIllegalStorageType.InheritStorageType.CompressedType.DeclareStorageType.BinaryType.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void declareStorageTypeWithOtherAnnotation() {
        declareStorageTypeWithSpaceId();
        declareStorageTypeWithSpaceRouting();
        declareStorageTypeWithSpaceIndex();

        declareStorageTypeWithSpaceExcludeAnnotation();
        declareStorageTypeWithSpaceDynamicPropertiesAnnotation();
        declareStorageTypeWithSpacePersistAnnotation();
        declareStorageTypeWithSpaceVesionAnnotation();
        declareStorageTypeWithSpaceLeaseExpirationAnnotation();
    }

    private void declareStorageTypeOnInheritedAnnotatedProperty() {
        declareStorageTypeOnInheritedSpaceId();
        declareStorageTypeOnInheritedSpaceRouting();
        declareStorageTypeOnInheritedSpaceIndex();

        declareStorageTypeOnInheritedSpaceDynamicPropertiesProperty();
        declareStorageTypeOnInheritedSpacePersistProperty();
        declareStorageTypeOnInheritedSpaceVesionProperty();
        declareStorageTypeOnInheritedSpaceLeaseExpirationProperty();
    }

    private void inheritStorageTypeAndDeclareAnnotation() {
        inheritStorageTypeDeclareSpaceId();
        inheritStorageTypeDeclareSpaceRouting();
        inheritStorageTypeDeclareSpaceIndex();

        inheritStorageTypeDeclareSpaceExclude();
        inheritStorageTypeDeclareSpaceDynamicProperties();
        inheritStorageTypeDeclareSpacePersistd();
        inheritStorageTypeDeclareSpaceVesion();
        inheritStorageTypeDeclareSpaceLeaseExpiration();

    }

    public void testIllegalPojoStorageTypeXml() {
        try {
            SpaceTypeInfoRepository.getTypeInfo(PojoIllegalStorageTypeXml.class);
            Assert.fail("Expected IllegalArgumentException.");
        } catch (IllegalArgumentException e) {
        }
    }

    public void IllegalStorageTypeDocument() {
        SpaceTypeDescriptorBuilder docBuilder =
                BuildBasicDocument("StorageTypeDocument", null)
                        .storageType(StorageType.BINARY)
                        .addFixedProperty("storageTypeProperty", Object.class, StorageType.COMPRESSED);
        ITypeDesc typeDesc = (ITypeDesc) docBuilder.create();

        try {
            String inheritTypeName = "InheritStorageTypeDocument";
            BuildBasicDocument(inheritTypeName, typeDesc).addFixedProperty("storageTypeProperty", Object.class, StorageType.BINARY).create();

            Assert.fail("Creating document with definition of storage type on inherited property yielded no exception");
        } catch (IllegalArgumentException e) {
        }

        try {
            String inheritTypeName = "InheritAndOverrideStorageTypeDocument";
            BuildBasicDocument(inheritTypeName, typeDesc).storageType(StorageType.COMPRESSED).create();

            Assert.fail("Creating document with definition of storage type on inherited class yielded no exception");
        } catch (IllegalStateException e) {
        }

        try {
            BuildBasicDocument("StorageTypeDoubleDefDocument", null)
                    .storageType(StorageType.BINARY)
                    .storageType(StorageType.COMPRESSED)
                    .create();

            Assert.fail("Creating document with two definitions of storage type yielded no exception");
        } catch (IllegalStateException e) {
        }

        try {
            BuildBasicDocument("StorageTypeOtherThanObjectOnPrimitiveDocument", null)
                    .addFixedProperty("bla", String.class, StorageType.COMPRESSED)
                    .create();

            Assert.fail("Creating document with definition of storage type on primitive yielded no exception");
        } catch (SpaceMetadataValidationException e) {
        }

        try {
            BuildBasicDocument("StorageTypeOtherThanObjectOnFifoGroupingPropertyDocument", null)
                    .fifoGroupingProperty("symbol")
                    .addFixedProperty("bla", Object.class, StorageType.COMPRESSED)
                    .addFifoGroupingIndex("bla")
                    .create();

            Assert.fail("Creating document with definition of storage type on fifo grouping property yielded no exception");
        } catch (SpaceMetadataValidationException e) {
        }

    }

    private void declareStorageTypeWithSpaceId() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.WithSpaceIdAnnotation.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.WithSpaceIdAnnotation.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void declareStorageTypeWithSpaceRouting() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.WithSpaceRoutingAnnotation.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.WithSpaceRoutingAnnotation.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void declareStorageTypeWithSpaceIndex() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.WithSpaceIndexAnnotation.BasicIndex.class,
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.WithSpaceIndexAnnotation.ExtendedIndexType.class,
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.WithSpaceIndexAnnotation.PathIndex.class,
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.WithSpaceIndexAnnotation.PathIndexWithCollection.class,
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.WithSpaceIndexAnnotation.PathIndexWithInnerCollection.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.WithSpaceIndexAnnotation.BasicIndex.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.WithSpaceIndexAnnotation.ExtendedIndexType.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.WithSpaceIndexAnnotation.PathIndex.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void declareStorageTypeWithSpaceExcludeAnnotation() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.DeclareStorageType.ObjectType.WithSpaceExcludeAnnotation.class,
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.WithSpaceExcludeAnnotation.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.WithSpaceExcludeAnnotation.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void declareStorageTypeWithSpaceDynamicPropertiesAnnotation() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.DeclareStorageType.ObjectType.WithSpaceDynamicPropertiesAnnotation.class,
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.WithSpaceDynamicPropertiesAnnotation.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.WithSpaceDynamicPropertiesAnnotation.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void declareStorageTypeWithSpacePersistAnnotation() {
        // OBJECT storage type
        // SpacePersist annotation can declared only on boolean property - no need to check other storage types
        // (Already checked in nonOBJECTStorageTypeOnPrimitiveProperty)
        try {
            SpaceTypeInfoRepository.getTypeInfo(PojoIllegalStorageType.DeclareStorageType.ObjectType.WithSpacePersistAnnotation.class);
            Assert.fail("Expected SpaceMetadataValidationException.");
        } catch (SpaceMetadataValidationException e) {
        }
    }

    private void declareStorageTypeWithSpaceVesionAnnotation() {
        // OBJECT storage type
        // SpaceVesion annotation can declared only on integer property - no need to check other storage types
        // (Already checked in nonOBJECTStorageTypeOnPrimitiveProperty)
        try {
            SpaceTypeInfoRepository.getTypeInfo(PojoIllegalStorageType.DeclareStorageType.ObjectType.WithSpaceVesionAnnotation.class);
            Assert.fail("Expected SpaceMetadataValidationException.");
        } catch (SpaceMetadataValidationException e) {
        }
    }

    private void declareStorageTypeWithSpaceLeaseExpirationAnnotation() {
        // OBJECT storage type
        // SpaceLeaseExpiration annotation can declared only on long property - no need to check other storage types
        // (Already checked in nonOBJECTStorageTypeOnPrimitiveProperty)
        try {
            SpaceTypeInfoRepository.getTypeInfo(PojoIllegalStorageType.DeclareStorageType.ObjectType.WithSpaceLeaseExpirationAnnotation.class);
            Assert.fail("Expected SpaceMetadataValidationException.");
        } catch (SpaceMetadataValidationException e) {
        }
    }

    private void declareStorageTypeOnInheritedSpaceId() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.InheritSpaceIdAnnotation.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.InheritSpaceIdAnnotation.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void declareStorageTypeOnInheritedSpaceRouting() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.InheritSpaceRoutingAnnotation.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.InheritSpaceRoutingAnnotation.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void declareStorageTypeOnInheritedSpaceIndex() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.InheritSpaceIndexAnnotation.BasicIndexType.class,
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.InheritSpaceIndexAnnotation.ExtendedIndexType.class,
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.InheritSpaceIndexAnnotation.PathIndex.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.InheritSpaceIndexAnnotation.BasicIndexType.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.InheritSpaceIndexAnnotation.ExtendedIndexType.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.InheritSpaceIndexAnnotation.PathIndex.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void declareStorageTypeOnInheritedSpaceDynamicPropertiesProperty() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.DeclareStorageType.ObjectType.InheritSpaceDynamicPropertiesAnnotation.class,
                        PojoIllegalStorageType.DeclareStorageType.BinaryType.InheritSpaceDynamicPropertiesAnnotation.class,
                        PojoIllegalStorageType.DeclareStorageType.CompressedType.InheritSpaceDynamicPropertiesAnnotation.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void declareStorageTypeOnInheritedSpacePersistProperty() {
        // OBJECT storage type
        // SpacePersist annotation can declared only on boolean property - no need to check other storage types
        // (Already checked in nonOBJECTStorageTypeOnPrimitiveProperty)
        try {
            SpaceTypeInfoRepository.getTypeInfo(PojoIllegalStorageType.DeclareStorageType.ObjectType.InheritSpacePersist.class);
            Assert.fail("Expected SpaceMetadataValidationException.");
        } catch (SpaceMetadataValidationException e) {
        }
    }

    private void declareStorageTypeOnInheritedSpaceVesionProperty() {
        // OBJECT storage type
        // SpaceVesion annotation can declared only on integer property - no need to check other storage types
        // (Already checked in nonOBJECTStorageTypeOnPrimitiveProperty)
        try {
            SpaceTypeInfoRepository.getTypeInfo(PojoIllegalStorageType.DeclareStorageType.ObjectType.InheritSpaceVesion.class);
            Assert.fail("Expected SpaceMetadataValidationException.");
        } catch (SpaceMetadataValidationException e) {
        }
    }

    private void declareStorageTypeOnInheritedSpaceLeaseExpirationProperty() {
        // OBJECT storage type
        // SpaceLeaseExpiration annotation can declared only on long property - no need to check other storage types
        // (Already checked in nonOBJECTStorageTypeOnPrimitiveProperty)
        try {
            SpaceTypeInfoRepository.getTypeInfo(PojoIllegalStorageType.DeclareStorageType.ObjectType.InheritSpaceLeaseExpiration.class);
            Assert.fail("Expected SpaceMetadataValidationException.");
        } catch (SpaceMetadataValidationException e) {
        }
    }

    private void inheritStorageTypeDeclareSpaceId() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.InheritStorageType.BinaryType.DeclareSpaceIdAnnotation.class,
                        PojoIllegalStorageType.InheritStorageType.CompressedType.DeclareSpaceIdAnnotation.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void inheritStorageTypeDeclareSpaceRouting() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.InheritStorageType.BinaryType.DeclareSpaceRoutingAnnotation.class,
                        PojoIllegalStorageType.InheritStorageType.CompressedType.DeclareSpaceRoutingAnnotation.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void inheritStorageTypeDeclareSpaceIndex() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.InheritStorageType.BinaryType.DeclareSpaceIndexAnnotation.BasicIndexType.class,
                        PojoIllegalStorageType.InheritStorageType.BinaryType.DeclareSpaceIndexAnnotation.ExtendedIndexType.class,
                        PojoIllegalStorageType.InheritStorageType.BinaryType.DeclareSpaceIndexAnnotation.PathIndex.class,
                        PojoIllegalStorageType.InheritStorageType.CompressedType.DeclareSpaceIndexAnnotation.BasicIndexType.class,
                        PojoIllegalStorageType.InheritStorageType.CompressedType.DeclareSpaceIndexAnnotation.ExtendedIndexType.class,
                        PojoIllegalStorageType.InheritStorageType.CompressedType.DeclareSpaceIndexAnnotation.PathIndex.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void inheritStorageTypeDeclareSpaceExclude() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.InheritStorageType.ObjectType.DeclareSpaceExcludeAnnotation.class,
                        PojoIllegalStorageType.InheritStorageType.BinaryType.DeclareSpaceExcludeAnnotation.class,
                        PojoIllegalStorageType.InheritStorageType.CompressedType.DeclareSpaceExcludeAnnotation.class,

                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void inheritStorageTypeDeclareSpaceDynamicProperties() {
        Class<?>[] types = new Class<?>[]
                {
                        PojoIllegalStorageType.InheritStorageType.ObjectType.DeclareSpaceDynamicPropertiesAnnotation.class,
                        PojoIllegalStorageType.InheritStorageType.BinaryType.DeclareSpaceDynamicPropertiesAnnotation.class,
                        PojoIllegalStorageType.InheritStorageType.CompressedType.DeclareSpaceDynamicPropertiesAnnotation.class
                };
        assertInvalidPojosThrowSpaceMetadataValidationException(types);
    }

    private void inheritStorageTypeDeclareSpacePersistd() {
        // OBJECT storage type
        // SpacePersist annotation can declared only on boolean property - no need to check other storage types
        // (Already checked in nonOBJECTStorageTypeOnPrimitiveProperty)
        try {
            SpaceTypeInfoRepository.getTypeInfo(PojoIllegalStorageType.InheritStorageType.ObjectType.DeclareSpacePersistAnnotation.class);
            Assert.fail("Expected SpaceMetadataValidationException.");
        } catch (SpaceMetadataValidationException e) {
        }
    }

    private void inheritStorageTypeDeclareSpaceVesion() {
        // OBJECT storage type
        // SpaceVesion annotation can declared only on integer property - no need to check other storage types
        // (Already checked in nonOBJECTStorageTypeOnPrimitiveProperty)
        try {
            SpaceTypeInfoRepository.getTypeInfo(PojoIllegalStorageType.InheritStorageType.ObjectType.DeclareSpaceVesionAnnotation.class);
            Assert.fail("Expected SpaceMetadataValidationException.");
        } catch (SpaceMetadataValidationException e) {
        }
    }

    private void inheritStorageTypeDeclareSpaceLeaseExpiration() {
        // OBJECT storage type
        // SpaceLeaseExpiration annotation can declared only on long property - no need to check other storage types
        // (Already checked in nonOBJECTStorageTypeOnPrimitiveProperty)
        try {
            SpaceTypeInfoRepository.getTypeInfo(PojoIllegalStorageType.InheritStorageType.ObjectType.DeclareSpaceLeaseExpirationAnnotation.class);
            Assert.fail("Expected SpaceMetadataValidationException.");
        } catch (SpaceMetadataValidationException e) {
        }
    }

    private void assertInvalidPojosThrowSpaceMetadataValidationException(Class<?>[] types) {
        // Assert:
        for (Class<?> type : types) {
            try {
                ITypeDesc typeDesc = new TypeDescFactory().createPojoTypeDesc(type, null, null);
                Exception EX = new SpaceMetadataValidationException(SpaceTypeInfoRepository.class, "");
                Assert.fail("Expected SpaceMetadataValidationException for type " + type.getName());
            } catch (SpaceMetadataValidationException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    ///////////////////////////////////
    //      FifoGrouping Tests       //
    ///////////////////////////////////

    public void testFifoGrouping() {
        fifoGroupingPojoTest(FifoGroupingBasicPojo.class, FifoGroupingBasicPojo.getFifoGroupingIndexes(), FifoGroupingBasicPojo.getIndexes(), FifoGroupingBasicPojo.getFifoGroupingPropertyName());
        fifoGroupingPojoTest(FifoGroupingBasicInheritPojo.class, FifoGroupingBasicInheritPojo.getFifoGroupingIndexes(), FifoGroupingBasicInheritPojo.getIndexes(), FifoGroupingBasicInheritPojo.getFifoGroupingPropertyName());
        fifoGroupingIllegalPojoTest();
        // fifo grouping gs xml Tests
        fifoGroupingPojoTest(PojoBasicFifoGroupingXml.class, PojoBasicFifoGroupingXml.getFifoGroupingIndexes(), PojoBasicFifoGroupingXml.getIndexes(), PojoBasicFifoGroupingXml.getFifoGroupingPropertyName());
        fifoGroupingPojoTest(PojoFifoGroupingInheritXml.class, PojoFifoGroupingInheritXml.getFifoGroupingIndexes(), PojoFifoGroupingInheritXml.getIndexes(), PojoFifoGroupingInheritXml.getFifoGroupingPropertyName());
        fifoGroupingIllegalXmlTest();

        fifoGroupingDocumentTest();
        fifoGroupingIllegalDocumentTest();
    }

    private void fifoGroupingPojoTest(Class<?> type, String[] expectedFifoGroupingIndexes, Map<String, SpaceIndexType> expectedIndexes, String fifoGroupingPropertyName) {
        ITypeDesc typeDesc = new TypeDescFactory().createPojoTypeDesc(type, null, null);
        SpaceTypeDescriptorTestCase.assertFifoGroupingProperty(typeDesc, fifoGroupingPropertyName);
        SpaceTypeDescriptorTestCase.assertFifoGroupingIndexes(typeDesc, expectedFifoGroupingIndexes);
        SpaceTypeDescriptorTestCase.assertIndexes(typeDesc, expectedIndexes);
    }

    private void fifoGroupingIllegalPojoTest() {
        Class<?>[] SpaceMetadataValidationExceptionExpectedTypes =
                {
                        FifoGroupsDoubleDefExceptionData.class,
                        FifoGroupsCollectionExceptionData.class,
                        FifoGroupsDoubleExceptionData.class,
                        FifoGroupingIllegalInheritPojo.class,
                        FifoGroupingIllegalPathInheritPojo.class,
                        FifoGroupingIllegalIndexOnCollection.class,
                        FifoGroupingIllegalSpaceExclude.class,
                        FifoGroupingIllegalIndexOnSpaceExclude.class
                };
        String[] SpaceMetadataValidationExceptionExpectedErrMessages =
                {
                        "Creating pojo with 2 definitions of fifogroups yielded no exception",
                        "Creating pojo with definition of fifogroups on collection yielded no exception",
                        "Creating pojo with 2 definitions of fifogroups, and one of them on collection yielded no exception",
                        "Creating pojo, declaring fifogrouping on inherited fifo grouping property yielded no exception",
                        "Creating pojo, declaring fifogrouping on inherited fifo grouping property with different path yielded no exception",
                        "Creating pojo with definition of SpaceFifoGroupingIndex on collection yielded no exception",
                        "Creating pojo with definition of SpaceFifoGroupingProperty on space exclude property yielded no exception",
                        "Creating pojo with definition of SpaceFifoGroupingIndex on space exclude property yielded no exception"
                };
        assertSpaceMetadataValidationExceptionExpected(SpaceMetadataValidationExceptionExpectedTypes, SpaceMetadataValidationExceptionExpectedErrMessages);

        Class<?>[] IllegalStateExceptionExpectedTypes =
                {
                        FifoGroupingIllegalIndexWithoutMain.class
                };
        String[] IllegalStateExceptionExpectedErrMessages =
                {
                        "Creating pojo with definition of fifoGroupingIndex but without fifo grouping property yielded no exception"
                };
        assertIllegalStateExceptionExpected(IllegalStateExceptionExpectedTypes, IllegalStateExceptionExpectedErrMessages);
    }

    private void fifoGroupingIllegalXmlTest() {
        Class<?>[] SpaceMetadataValidationExceptionExpectedTypes =
                {
                        PojoIllegalFifoGroupingDuplicateXml.class, // 1
                        PojoIllegalFifoGroupingDuplicatePathXml.class, // 2
                        PojoIllegalFifoGroupingOnCollectionXml.class, // 3
                        PojoIllegalFifoGroupingOnCollectionDoubleDefXml.class, // 4
                        PojoIllegalFifoGroupingInheritDubleDefXml.class, // 5
                        PojoIllegalFifoGroupingInheritDubleDefWithPathXml.class, // 6
                        PojoFifoGroupingIllegalIndexOnCollectionXml.class, // 7
                        PojoIllegalFifoGroupingPropertyOnSpaceExcludeXml.class, // 8
                        PojoIllegalFifoGroupingIndexOnSpaceExcludeXml.class // 9
                };
        String[] SpaceMetadataValidationExceptionExpectedErrMessages =
                {
                        "Creating pojo via xml with 2 definitions of fifogroups yielded no exception", // 1
                        "Creating pojo via xml with 2 definitions of fifogroups on the same property with different path yielded no exception", // 2
                        "Creating pojo via xml with definition of fifogroups on collection yielded no exception", // 3
                        "Creating pojo via xml with 2 definitions of fifogroups, and one of them on collection yielded no exception", // 4
                        "Creating pojo via xml, declaring fifogrouping when one is allready exists in the super class yielded no exception", // 5
                        "Creating pojo via xml, declaring fifogrouping on inherited fifo grouping property with different path yielded no exception", // 6
                        "Creating pojo via xml with definition of SpaceFifoGroupingIndex on collection yielded no exception", // 7
                        "Creating pojo via xml with definition of fifoGroupingProperty on space exclude property yielded no exception", // 8
                        "Creating pojo via xml with definition of fifoGroupingIndex on space exclude property yielded no exception" // 9
                };

        assertSpaceMetadataValidationExceptionExpected(SpaceMetadataValidationExceptionExpectedTypes, SpaceMetadataValidationExceptionExpectedErrMessages);


        Class<?>[] IllegalStateExceptionExpectedTypes =
                {
                        PojoIllegalFifoGroupingIndexWithoutMainXml.class
                };
        String[] IllegalStateExceptionExpectedErrMessages =
                {
                        "Creating pojo via xml with definition of fifoGroupingIndex but without fifo grouping property yielded no exception"
                };
        assertIllegalStateExceptionExpected(IllegalStateExceptionExpectedTypes, IllegalStateExceptionExpectedErrMessages);
    }

    private void assertSpaceMetadataValidationExceptionExpected(Class<?>[] types, String[] errMessages) {
        for (int i = 0; i < types.length; i++) {
            try {
                ITypeDesc typeDesc = new TypeDescFactory().createPojoTypeDesc(types[i], null, null);
                Assert.fail("[" + typeDesc.getTypeName() + "] " + errMessages[i]);
            } catch (SpaceMetadataValidationException e) {
            }
        }
    }

    private void assertIllegalStateExceptionExpected(Class<?>[] types, String[] errMessages) {
        for (int i = 0; i < types.length; i++) {
            try {
                ITypeDesc typeDesc = new TypeDescFactory().createPojoTypeDesc(types[i], null, null);
                Assert.fail("[" + typeDesc.getTypeName() + "] " + errMessages[i]);
            } catch (IllegalStateException e) {
            }
        }
    }

    private void fifoGroupingDocumentTest() {
        String typeName = "FifoGroupingDocument";
        SpaceTypeDescriptorBuilder docBuilder =
                BuildBasicDocument(typeName, null)
                        .fifoGroupingProperty("symbol")
                        .addFifoGroupingIndex("reporter")
                        .addFifoGroupingIndex("processed")
                        .addFifoGroupingIndex("info")
                        .addFifoGroupingIndex("time.nanos")
                        .addPathIndex("processed", SpaceIndexType.EQUAL_AND_ORDERED)
                        .addPathIndex("info.timeStamp", SpaceIndexType.EQUAL_AND_ORDERED)
                        .addPathIndex("formerReporters", SpaceIndexType.EQUAL)
                        .addPathIndex("time.nanos", SpaceIndexType.EQUAL);
        ITypeDesc typeDesc = (ITypeDesc) docBuilder.create();

        assertFifoGroupingProperty(typeDesc, "symbol");
        assertFifoGroupingIndexes(typeDesc, FifoGroupingBasicPojo.getFifoGroupingIndexes());
        Map<String, SpaceIndexType> indexes = FifoGroupingBasicPojo.getIndexes();
        assertIndexes(typeDesc, indexes);


        String inheritTypeName = "FifoGroupingInheritDocument";
        SpaceTypeDescriptorBuilder inheritDocBuilder =
                BuildBasicDocument(inheritTypeName, typeDesc)
                        .addFifoGroupingIndex("info.scans");
        ITypeDesc inheritTypeDesc = (ITypeDesc) inheritDocBuilder.create();

        assertFifoGroupingProperty(inheritTypeDesc, "symbol");
        assertFifoGroupingIndexes(inheritTypeDesc, FifoGroupingBasicInheritPojo.getFifoGroupingIndexes());
        indexes.put("info.scans", SpaceIndexType.EQUAL);
        assertIndexes(inheritTypeDesc, indexes);
    }

    private void fifoGroupingIllegalDocumentTest() {
        try {
            String typeName = "FifoGroupingDoubleDefDocument";
            SpaceTypeDescriptorBuilder docBuilder =
                    BuildBasicDocument(typeName, null)
                            .fifoGroupingProperty("symbol")
                            .fifoGroupingProperty("reporter");
            docBuilder.create();
            Assert.fail("Creating document with 2 definitions of fifogroups yielded no exception");
        } catch (IllegalStateException e) {
        }

        try {
            String typeName = "FifoGroupingDoubleDefOnSamePropertyDocument";
            SpaceTypeDescriptorBuilder docBuilder =
                    BuildBasicDocument(typeName, null)
                            .fifoGroupingProperty("info.scans")
                            .fifoGroupingProperty("info.lastReports");
            docBuilder.create();
            Assert.fail("Creating document with 2 definitions of fifogroups (same property, different path) yielded no exception");
        } catch (IllegalStateException e) {
        }

        try {
            String typeName = "FifoGroupingOnCollectionDocument";
            SpaceTypeDescriptorBuilder docBuilder =
                    BuildBasicDocument(typeName, null)
                            .fifoGroupingProperty("reportersList.[*]");
            docBuilder.create();
            Assert.fail("Creating document with definition of fifogroups on collection yielded no exception");
        } catch (IllegalArgumentException e) {
        }

        try {
            String typeName = "FifoGroupingDoubleDefOnCollectionDocument";
            SpaceTypeDescriptorBuilder docBuilder =
                    BuildBasicDocument(typeName, null)
                            .fifoGroupingProperty("symbol")
                            .fifoGroupingProperty("reportersList.[*]");
            docBuilder.create();
            Assert.fail("Creating document with 2 definitions of fifogroups, and one of them on collection yielded no exception");
        } catch (IllegalStateException e) {
        }

        try {
            String typeName = "FifoGroupingDocument";
            SpaceTypeDescriptorBuilder docBuilder =
                    BuildBasicDocument(typeName, null)
                            .fifoGroupingProperty("symbol");
            SpaceTypeDescriptor FifoGroupingTypeDescriptor = docBuilder.create();

            String inheritTypeName = "FifoGroupingInheritAndDeclareFifoGroupingOnDifferentPropertyDocument";
            SpaceTypeDescriptorBuilder inheritDocBuilder =
                    BuildBasicDocument(inheritTypeName, FifoGroupingTypeDescriptor)
                            .fifoGroupingProperty("reporter");
            inheritDocBuilder.create();

            Assert.fail("Creating document, declaring fifogrouping on inherited fifo grouping property yielded no exception");
        } catch (IllegalStateException e) {
        }

        try {
            String typeName = "FifoGroupingDocument";
            SpaceTypeDescriptorBuilder docBuilder =
                    BuildBasicDocument(typeName, null)
                            .fifoGroupingProperty("info.lastReports");
            SpaceTypeDescriptor FifoGroupingTypeDescriptor = docBuilder.create();

            String inheritTypeName = "FifoGroupingInheritAndDeclareFifoGroupingOnTheSamePropertyDocument";
            SpaceTypeDescriptorBuilder inheritDocBuilder =
                    BuildBasicDocument(inheritTypeName, FifoGroupingTypeDescriptor)
                            .fifoGroupingProperty("info.scans");
            inheritDocBuilder.create();

            Assert.fail("Creating document, declaring fifogrouping on inherited fifo grouping property with different path yielded no exception");
        } catch (IllegalStateException e) {
        }

        try {
            String typeName = "FifoGroupingIndexWithoutMainDocument";
            SpaceTypeDescriptorBuilder docBuilder =
                    BuildBasicDocument(typeName, null)
                            .addFifoGroupingIndex("symbol");
            docBuilder.create();

            Assert.fail("Creating document with definition of fifoGroupingIndex but without fifo grouping property yielded no exception");
        } catch (IllegalStateException e) {
        }

        try {
            String typeName = "FifoGroupingIndexOnCollectionDocument";
            SpaceTypeDescriptorBuilder docBuilder =
                    BuildBasicDocument(typeName, null)
                            .fifoGroupingProperty("symbol")
                            .addFifoGroupingIndex("reportersList.[*]");
            docBuilder.create();

            Assert.fail("Creating document with definition of SpaceFifoGroupingIndex on collection yielded no exception");
        } catch (IllegalArgumentException e) {
        }
    }

    public static SpaceTypeDescriptorBuilder BuildBasicDocument(String typeName, SpaceTypeDescriptor superTypeDescriptor) {
        SpaceTypeDescriptorBuilder docBuilder = new SpaceTypeDescriptorBuilder(typeName, superTypeDescriptor);
        if (superTypeDescriptor != null)
            return docBuilder;
        return docBuilder
                .addFixedProperty("id", String.class)
                .addFixedProperty("symbol", String.class)
                .addFixedProperty("reporter", String.class)
                .addFixedProperty("processed", Boolean.class)
                .addFixedProperty("info", FifoGroupingBasicPojo.Info.class)
                .addFixedProperty("timeStamp", Timestamp.class)
                .addFixedProperty("reportersList", List.class)
                .idProperty("id");
    }

    public static void assertIndexes(ITypeDesc typeDesc, Map<String, SpaceIndexType> expectedIndexes) {
        Map<String, SpaceIndex> indexes = typeDesc.getIndexes();
        Assert.assertTrue(indexes.size() == expectedIndexes.size());
        for (String indexName : expectedIndexes.keySet()) {
            SpaceIndex spaceIndex = indexes.get(indexName);
            Assert.assertNotNull(spaceIndex);
            Assert.assertTrue(spaceIndex.getIndexType() == expectedIndexes.get(indexName));
        }
    }

    public static void assertFifoGroupingIndexes(ITypeDesc typeDesc, String[] expectedFifoGroupingIndexes) {
        Set<String> fifoGroupingIndexNames = typeDesc.getFifoGroupingIndexesPaths();
        Assert.assertTrue(fifoGroupingIndexNames.size() == expectedFifoGroupingIndexes.length);
        for (String fifoGroupingIndexeName : expectedFifoGroupingIndexes)
            Assert.assertTrue(fifoGroupingIndexNames.contains(fifoGroupingIndexeName));
    }

    public static void assertFifoGroupingProperty(ITypeDesc typeDesc, String propertyName) {
        String fifoGroupingName = typeDesc.getFifoGroupingPropertyPath();
        Assert.assertNotNull(fifoGroupingName);
        Assert.assertTrue(fifoGroupingName.equals(propertyName) || fifoGroupingName.startsWith(propertyName + "."));
        Assert.assertNotNull(typeDesc.getIndexes().get(fifoGroupingName));
    }

}
