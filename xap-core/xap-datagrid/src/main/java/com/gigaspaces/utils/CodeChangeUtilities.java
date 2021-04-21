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
package com.gigaspaces.utils;

import com.gigaspaces.annotation.SupportCodeChange;
import com.gigaspaces.internal.classloader.ClassLoaderCache;
import org.jini.rio.boot.SupportCodeChangeAnnotationContainer;

import java.util.Collection;

/**
 * Created by tamirs
 * on 12/8/16.
 */
public class CodeChangeUtilities {

    public static void removeOneTimeClassLoaderIfNeeded(Collection objects) {
        for (Object object : objects) {
            ClassLoader oneTimeClassLoader = getOneTimeClassLoader(object);
            if(oneTimeClassLoader != null){
                ClassLoaderCache.getCache().removeClassLoader(oneTimeClassLoader);
            }
        }
    }

    private static ClassLoader getOneTimeClassLoader(Object object) {
        if(object.getClass().isAnnotationPresent(SupportCodeChange.class)){
            SupportCodeChange annotation = object.getClass().getAnnotation(SupportCodeChange.class);
            if(annotation.id().isEmpty()){
                return object.getClass().getClassLoader();
            }
        }
        return null;
    }

    public static SupportCodeChangeAnnotationContainer createContainerFromSupportCodeAnnotationIfNeeded(Object object) {
        SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer = null;
        Class<?> objectClass = object.getClass();
        if(objectClass.isAnnotationPresent(SupportCodeChange.class)) {
            SupportCodeChange annotation = objectClass.getAnnotation(SupportCodeChange.class);
            if (annotation.id().isEmpty()) {
                supportCodeChangeAnnotationContainer = SupportCodeChangeAnnotationContainer.ONE_TIME;
            } else {
                supportCodeChangeAnnotationContainer = new SupportCodeChangeAnnotationContainer(annotation.id());
            }
        }
        return supportCodeChangeAnnotationContainer;
    }
}
