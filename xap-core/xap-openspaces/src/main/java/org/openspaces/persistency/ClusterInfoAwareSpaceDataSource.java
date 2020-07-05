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

package org.openspaces.persistency;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceInitialLoadQuery;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.internal.utils.ReflectionUtils;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.TypeFilter;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by yuvalm on 23/04/2014.
 *
 * @since 10.0
 */
public abstract class ClusterInfoAwareSpaceDataSource extends SpaceDataSource implements ClusterInfoAware {

    protected String[] initialLoadQueryScanningBasePackages = null;
    protected ClusterInfo clusterInfo;
    protected Map<String, String> initialLoadQueries = new HashMap<>();
    protected Map<String, List<String>> initialLoadChunksRoutingQueries = new HashMap<>();
    protected boolean augmentInitialLoadEntries = true;

    public void setInitialLoadQueryScanningBasePackages(String[] initialLoadQueryScanningBasePackages) {
        this.initialLoadQueryScanningBasePackages = initialLoadQueryScanningBasePackages;
    }

    @Override
    public DataIterator<SpaceTypeDescriptor> initialMetadataLoad() {
        initialLoadQueries.clear();
        return super.initialMetadataLoad();
    }

    @Override
    public void setClusterInfo(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

    public Map<String, String> getInitialLoadQueries() {
        return initialLoadQueries;
    }

    private boolean isClassMatch(MetadataReader metadataReader, ClassLoader classLoader) {
        final Class<?> clazz;
        try {
            clazz = classLoader.loadClass(metadataReader.getClassMetadata().getClassName());
        } catch (ClassNotFoundException e) {
            // shouldn't happen...
            throw new IllegalStateException("Classpath scanning error", e);
        }
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(SpaceInitialLoadQuery.class)) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method scans the classpath using Spring's {@link org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider}.
     * This scanner uses the logic in the #isClassMatch method to retrieve names of classes that
     * contain a method annotated with {@link com.gigaspaces.annotation.pojo.SpaceInitialLoadQuery}.
     * This collection of class names is then handled in such a way that each annotated method is
     * validated, examined and eventually invoked to obtain the specific type's initial load query.
     * <p></p>
     */
    protected void obtainInitialLoadQueries() {
        // temporary classloader that loads every class by name and is discarded when scanning the classpath is done.
        final ClassLoader classLoader = new ClassLoader(getClass().getClassLoader()) {
        };
        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new TypeFilter() {
            @Override
            public boolean match(MetadataReader metadataReader, MetadataReaderFactory metadataReaderFactory) throws IOException {
                return isClassMatch(metadataReader, classLoader);
            }
        });
        if (initialLoadQueryScanningBasePackages == null) {
            return;
        }
        for (String basePackage : initialLoadQueryScanningBasePackages) {
            Set<BeanDefinition> candidates = scanner.findCandidateComponents(basePackage);
            for (final BeanDefinition beanDefinition : candidates) {
                final Class<?> clazz;
                try {
                    clazz = classLoader.loadClass(beanDefinition.getBeanClassName());
                } catch (ClassNotFoundException e) {
                    // shouldn't happen
                    throw new IllegalStateException("Cannot access class " + beanDefinition.getBeanClassName(), e);
                }
                ReflectionUtils.doWithMethods(clazz, new ReflectionUtils.MethodCallback() {
                    @Override
                    public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
                        processMethod(method, clazz);
                    }
                }, new ReflectionUtils.MethodFilter() {
                    @Override
                    public boolean matches(Method method) {
                        return method.isAnnotationPresent(SpaceInitialLoadQuery.class);
                    }
                });
            }
        }
    }

    private void processMethod(Method method, Class<?> clazz) {
        String type = processMethodAnnotation(method, clazz);
        boolean requiresClusterInfo = false;
        Class<?>[] paramTypes = method.getParameterTypes();
        if (paramTypes.length > 0) {
            if (paramTypes.length == 1 && paramTypes[0].equals(ClusterInfo.class)) {
                requiresClusterInfo = true;
            } else {
                // can't invoke!
                throw new IllegalStateException("Initial load query method receives unexpected parameters (expected none or ClusterInfo only) - " + method);
            }
        }
        Object obj = null;
        if (!Modifier.isStatic(method.getModifiers())) {
            try {
                obj = clazz.newInstance();
            } catch (InstantiationException e) {
                throw new IllegalStateException("Class " + clazz.getName() + " has no public zero-parameter constructor! Cannot invoke initial load query method " + method.getName());
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Class " + clazz.getName() + " has no public zero-parameter constructor! Cannot invoke initial load query method " + method.getName());
            }
        }
        String query;
        try {
            if (requiresClusterInfo) {
                query = (String) method.invoke(obj, clusterInfo);
            } else {
                query = (String) method.invoke(obj);
            }
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Initial load query method has thrown an exception: " + method, e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot invoke initial load query method because it is not public: " + method, e);
        } catch (ClassCastException e) {
            throw new IllegalStateException("Initial load query method returns unexpected type (expected String) - " + method);
        }
        initialLoadQueries.put(type, query);
    }

    private String processMethodAnnotation(Method method, Class<?> clazz) {
        SpaceInitialLoadQuery queryAnnotation = method.getAnnotation(SpaceInitialLoadQuery.class);
        String type = queryAnnotation.type();
        if (type == null || type.isEmpty()) {
            if (!clazz.isAnnotationPresent(SpaceClass.class)) {
                throw new IllegalStateException(SpaceInitialLoadQuery.class.getName() +
                        " annotation without 'type' argument appears in class " + clazz.getName() +
                        " which is not annotated as " + SpaceClass.class.getName());
            } else {
                type = clazz.getName();
            }
        }
        if (initialLoadQueries.containsKey(type)) {
            // fail fast
            throw new IllegalArgumentException("There is more than one initial load query for type " + type);
        }
        return type;
    }

    protected String createInitialLoadQuery(SpaceTypeDescriptor typeDescriptor, String templateQuery) {
        SpacePropertyDescriptor routingPropertyType = typeDescriptor.getFixedProperty(typeDescriptor.getRoutingPropertyName());
        if (routingPropertyType == null) {
            // can't move on without a routing property
            return null;
        }
        Class<?> routingPropertyClass = routingPropertyType.getType();
        if (!ReflectionUtils.isNumeric(routingPropertyClass)) {
            // routing property type is non-numeric... log?
            return null;
        }
        // routing property is numeric => go ahead
        return templateQuery.replace("?", routingPropertyType.getName());
    }

}