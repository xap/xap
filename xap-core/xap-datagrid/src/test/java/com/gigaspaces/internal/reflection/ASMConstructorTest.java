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

package com.gigaspaces.internal.reflection;

import com.gigaspaces.internal.reflection.fast.ASMConstructorFactory;
import com.gigaspaces.internal.reflection.standard.StandardConstructor;
import com.gigaspaces.internal.reflection.standard.StandardMethod;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;

/**
 * @author assafr
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class ASMConstructorTest {

    @Test
    public void testClass() {
        testClass(PublicClass.class, false);
    }

    @Test
    public void testPrivateClassPrivateCtor() {
        testClass(PrivateClassPrivateCtor.class, true);
    }

    @Test
    public void testPublicConstructor() {
        testClass(PrivateCtorClass.class, true);
    }

    @Test
    public void testPrivateConstructor() {
        testClass(PrivateClassPublicCtor.class, false);
    }

    @Test
    public void testPackagedConstructor() {
        testClass(PackagedClassPackagedCtor.class, false);
    }

    @Test
    public void testProtectedConstructor() {
        testClass(ProtectedClassProtectedCtor.class, false);
    }

    /**
     * Checks that repeat call returns the same class
     */
    @Test
    public void testRepeatGet() throws Exception {
        Constructor<PublicClass> ctor = PublicClass.class.getConstructor();
        IConstructor<PublicClass> iCtor = ASMConstructorFactory.getConstructor(ctor);
        Assert.assertNotSame(iCtor.getClass(), StandardMethod.class);
        Assert.assertEquals(iCtor.getClass(), ASMConstructorFactory.getConstructor(ctor).getClass());
        Assert.assertEquals(iCtor.getClass(), ASMConstructorFactory.getConstructor(ctor).getClass());
    }

    private <T> void testClass(Class<T> clazz, boolean expectedStandard) {
        IConstructor<T> iCtor = ReflectionUtil.createCtor(clazz);
        Object obj = iCtor.newInstance();
        Assert.assertNotNull(obj);
        if (expectedStandard)
            Assert.assertSame(StandardConstructor.class, iCtor.getClass());
        else
            Assert.assertNotSame(StandardConstructor.class, iCtor.getClass());
    }

    public static class PublicClass {
        public PublicClass() {
        }
    }

    private static class PrivateClassPrivateCtor {
        private PrivateClassPrivateCtor() {
        }
    }

    public static class PrivateCtorClass {
        private PrivateCtorClass() {
        }
    }

    private static class PrivateClassPublicCtor {
        public PrivateClassPublicCtor() {
        }
    }

    static class PackagedClassPackagedCtor {
        PackagedClassPackagedCtor() {
        }
    }

    protected static class ProtectedClassProtectedCtor {
        protected ProtectedClassProtectedCtor() {
        }
    }
}
