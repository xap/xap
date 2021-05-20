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

package com.gigaspaces.persistency.qa.utest.metadata;


import com.gigaspaces.internal.reflection.IGetterMethod;
import com.gigaspaces.internal.reflection.ISetterMethod;
import com.gigaspaces.persistency.metadata.PojoRepository;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;

public class PojoRepositoryTest {

    private static final int NUMBER = 27017;
    private static final String MY_NAME = "My Name";

    public static class PojoA {
        private String nameA;

        public PojoA() {
        }

        public String getNameA() {
            return nameA;
        }

        public void setNameA(String nameA) {
            this.nameA = nameA;
        }

    }

    public static class PojoB extends PojoA {
        private int numberB;

        public PojoB() {
        }

        public int getNumberB() {
            return numberB;
        }

        public void setNumberB(int numberB) {
            this.numberB = numberB;
        }
    }

    private PojoRepository repository;

    @Before
    public void before() {
        repository = new PojoRepository();

        repository.introcpect(PojoA.class);
        repository.introcpect(PojoB.class);

    }

    @Test
    public void testConstructor() {

        PojoA pojoA = (PojoA) repository.getConstructor(PojoA.class)
                .newInstance();
        Assert.assertNotNull(pojoA);

        PojoB pojoB = (PojoB) repository.getConstructor(PojoB.class)
                .newInstance();

        Assert.assertNotNull(pojoB);
    }

    @Test
    public void testGetter() throws InvocationTargetException, IllegalAccessException {

        PojoB pojoB = (PojoB) repository.getConstructor(PojoB.class)
                .newInstance();

        IGetterMethod<Object> nameA = repository
                .getGetter(PojoB.class, "nameA");
        IGetterMethod<Object> numberB = repository.getGetter(PojoB.class,
                "numberB");

        Assert.assertNull(nameA.get(pojoB));
        Assert.assertTrue(numberB.get(pojoB).equals(0));

        pojoB.setNameA(MY_NAME);
        pojoB.setNumberB(NUMBER);

        Assert.assertTrue(nameA.get(pojoB).equals(MY_NAME));
        Assert.assertTrue(numberB.get(pojoB).equals(NUMBER));

    }

    @Test
    public void testSetter() throws InvocationTargetException, IllegalAccessException {
        PojoB pojoB = (PojoB) repository.getConstructor(PojoB.class)
                .newInstance();

        ISetterMethod<Object> nameA = repository
                .getSetter(PojoB.class, "nameA");

        ISetterMethod<Object> numberB = repository.getSetter(PojoB.class,
                "numberB");

        nameA.set(pojoB, MY_NAME);
        numberB.set(pojoB, NUMBER);

        Assert.assertTrue(pojoB.getNameA().equals(MY_NAME));
        Assert.assertTrue(pojoB.getNumberB() == NUMBER);
    }

    @Test(expected = IllegalStateException.class)
    public void testUndefinePrperty() {
        repository.getSetter(PojoB.class, "nameAAAAAAAAAAAAAAAAAAAAAAAAA");
    }

}