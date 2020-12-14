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

package com.gigaspaces.internal.query.predicate;

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.metadata.DotNetStorageType;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.internal.query.predicate.comparison.EqualsSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.NotEqualsSpacePredicate;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryPathGetter;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryPropertyGetter;
import com.gigaspaces.internal.server.storage.FlatEntryData;
import com.gigaspaces.metadata.SpaceDocumentSupport;
import com.gigaspaces.metadata.StorageType;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.client.ExternalEntry;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class SpaceValueGettersTests {
    @Test
    public void testSpaceEntryPropertyGetter() {
        String[] names = new String[]{"a", "b"};
        Object[] values = new Object[]{"bar", 1};
        Map<String, Object> dynamicProperties = null;

        ITypeDesc typeDesc = createMockTypeDesc(names);
        ServerEntry entry = new FlatEntryData(values, dynamicProperties, typeDesc.getEntryTypeDesc(EntryType.DOCUMENT_JAVA), 0, 0, null);

        for (int i = 0; i < names.length; i++)
            testSpaceEntryPropertyGetter(names[i], entry, values[i]);
    }

    private void testSpaceEntryPropertyGetter(String name, ServerEntry entry, Object expected) {
        // Arrange:
        SpaceEntryPropertyGetter getter = new SpaceEntryPropertyGetter(name);
        // Act:
        Object actual = getter.getValue(entry);
        // Assert:
        Assert.assertEquals(expected, actual);

        AssertEx.assertExternalizable(getter);
    }

    @Test
    public void testSpaceEntryPathGetter() {
        String[] names = new String[]{"name", "address"};
        Object[] values = new Object[]{new Name("kermit", "the frog"), new Address(new Street("sesame", 7))};
        Map<String, Object> dynamicProperties = null;

        ITypeDesc typeDesc = createMockTypeDesc(names);
        ServerEntry entry = new FlatEntryData(values, dynamicProperties, typeDesc.getEntryTypeDesc(EntryType.DOCUMENT_JAVA), 0, 0, null);

        testSpaceEntryPathGetter("name.first", entry, "kermit");
        testSpaceEntryPathGetter("name.last", entry, "the frog");
        testSpaceEntryPathGetter("address.street.name", entry, "sesame");
        testSpaceEntryPathGetter("address.street.number", entry, 7);
    }

    private void testSpaceEntryPathGetter(String path, ServerEntry entry, Object expected) {
        // Arrange:
        SpaceEntryPathGetter getter = new SpaceEntryPathGetter(path);

        // Act:
        Object actual = getter.getValue(entry);

        // Assert:
        Assert.assertEquals(expected, actual);

        AssertEx.assertExternalizable(getter);
    }

    @Test
    public void testValueGetterSpacePredicate() {
        // Arrange:
        String[] names = new String[]{"a", "b"};
        Object[] values = new Object[]{"bar", 1};
        Map<String, Object> dynamicProperties = null;

        ITypeDesc typeDesc = createMockTypeDesc(names);
        ServerEntry entry = new FlatEntryData(values, dynamicProperties, typeDesc.getEntryTypeDesc(EntryType.DOCUMENT_JAVA), 0, 0, null);

        for (int i = 0; i < names.length; i++)
            testValueGetterSpacePredicate(names[i], entry, new EqualsSpacePredicate(values[i]), new NotEqualsSpacePredicate(values[i]));
    }

    private void testValueGetterSpacePredicate(String name, ServerEntry entry, ISpacePredicate validPredicate, ISpacePredicate invalidPredicate) {
        // Arrange:
        SpaceEntryPropertyGetter getter = new SpaceEntryPropertyGetter(name);

        ValueGetterSpacePredicate<ServerEntry> p1 = new ValueGetterSpacePredicate<ServerEntry>(getter, validPredicate);
        boolean result1 = p1.execute(entry);
        Assert.assertTrue(result1);

        ValueGetterSpacePredicate<ServerEntry> p2 = new ValueGetterSpacePredicate<ServerEntry>(getter, invalidPredicate);
        boolean result2 = p2.execute(entry);
        Assert.assertFalse(result2);

        AssertEx.assertExternalizable(p1);
        AssertEx.assertExternalizable(p2);
    }

    private ITypeDesc createMockTypeDesc(String[] propertyNames) {
        String className = "someclass";
        String codeBase = null;
        String[] superClasses = null;
        PropertyInfo[] properties = new PropertyInfo[propertyNames.length];
        for (int i = 0; i < properties.length; i++) {
            properties[i] = PropertyInfo.builder(propertyNames[i])
                    .type("")
                    .storageType(StorageType.OBJECT)
                    .build();
        }
        boolean supportsDynamicProperties = false;
        boolean isSystemType = false;
        String identifierPropertyName = null;
        String defaultPropertyName = null;
        String routingPropertyName = null;
        FifoSupport fifoMode = null;
        boolean replicable = false;
        boolean supportsOptimisticLocking = false;
        EntryType entryType = EntryType.OBJECT_JAVA;
        Class<Object> objectClass = null;
        Map<String, SpaceIndex> indexes = new HashMap<String, SpaceIndex>();
        boolean idAutoGenerate = false;
        return new TypeDesc(className, codeBase, superClasses, properties, supportsDynamicProperties,
                indexes, identifierPropertyName, idAutoGenerate, defaultPropertyName, routingPropertyName, null, null,
                isSystemType, fifoMode, replicable, supportsOptimisticLocking, StorageType.OBJECT, entryType,
                objectClass, ExternalEntry.class, SpaceDocument.class, null, DotNetStorageType.NULL, false, null, null,null);
    }

    public static class Name {
        private String _first;
        private String _last;

        public Name() {
        }

        public Name(String first, String last) {
            this._first = first;
            this._last = last;
        }

        public String getFirst() {
            return _first;
        }

        public String getLast() {
            return _last;
        }
    }

    public static class Address {
        private Street _street;

        public Address() {
        }

        public Address(Street street) {
            this._street = street;
        }

        public Street getStreet() {
            return _street;
        }
    }

    public static class Street {
        private String _name;
        private int _number;

        public Street() {
        }

        public Street(String name, int number) {
            this._name = name;
            this._number = number;
        }

        public String getName() {
            return _name;
        }

        public int getNumber() {
            return _number;
        }
    }
}
