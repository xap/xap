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
package com.gigaspaces.internal.services;

import com.gigaspaces.classloader.CustomURLClassLoader;
import com.gigaspaces.start.ClasspathBuilder;

import java.io.Closeable;

/**
 * @author Niv Ingberg
 * @since 12.1
 */
public abstract class ServiceFactory {
    public Closeable createService() {
        final String serviceClassName = getServiceClassName();
        final ClasspathBuilder classpathBuilder = new ClasspathBuilder();
        initializeClasspath(classpathBuilder);

        final ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            CustomURLClassLoader classLoader = new CustomURLClassLoader(getServiceName(), classpathBuilder.toURLsArray(), origClassLoader);
            Thread.currentThread().setContextClassLoader(classLoader);
            final Class<?> serviceClass = classLoader.loadClass(serviceClassName);
            final Object service = serviceClass.newInstance();
            return (Closeable) service;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create an instance of " + serviceClassName, e);
        } finally {
            Thread.currentThread().setContextClassLoader(origClassLoader);
        }
    }

    public abstract String getServiceName();

    protected abstract String getServiceClassName();

    protected abstract void initializeClasspath(ClasspathBuilder classpath);
}

