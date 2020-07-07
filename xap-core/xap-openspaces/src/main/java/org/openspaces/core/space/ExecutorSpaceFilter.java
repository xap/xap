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

package org.openspaces.core.space;

import com.gigaspaces.internal.reflection.IField;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.filters.FilterOperationCodes;
import com.j_spaces.core.filters.ISpaceFilter;
import com.j_spaces.core.filters.entry.ISpaceFilterEntry;

import net.jini.core.entry.UnusableEntryException;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.openspaces.core.executor.AutowireTask;
import org.openspaces.core.executor.AutowireTaskMarker;
import org.openspaces.core.executor.TaskGigaSpace;
import org.openspaces.core.executor.TaskGigaSpaceAware;
import org.openspaces.core.executor.internal.InternalSpaceTaskWrapper;
import org.openspaces.core.executor.support.DelegatingTask;
import org.openspaces.core.executor.support.ProcessObjectsProvider;
import org.openspaces.remoting.ExecutorRemotingTask;
import org.openspaces.remoting.SpaceRemotingServiceExporter;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ExecutorSpaceFilter implements ISpaceFilter {

    private static Object NO_FIELD = new Object();

    private final Map<Class, Object> tasksGigaSpaceInjectionMap = new CopyOnUpdateMap<Class, Object>();
    private final AbstractSpaceFactoryBean spaceFactoryBean;
    private ClusterInfo clusterInfo;
    private IJSpace space;
    private GigaSpace gigaSpace;
    private final Object lock = new Object();
    private volatile SpaceRemotingServiceExporter serviceExporter;

    public ExecutorSpaceFilter(AbstractSpaceFactoryBean spaceFactoryBean, ClusterInfo clusterInfo) {
        this.spaceFactoryBean = spaceFactoryBean;
        this.clusterInfo = clusterInfo;
    }

    public void init(IJSpace space, String filterId, String url, int priority) throws RuntimeException {
        this.space = space;
        this.gigaSpace = new GigaSpaceConfigurer(space).gigaSpace();
    }

    public void process(SpaceContext context, ISpaceFilterEntry entry, int operationCode) throws RuntimeException {
        if (operationCode != FilterOperationCodes.BEFORE_EXECUTE) {
            return;
        }
        ApplicationContext applicationContext = spaceFactoryBean.getApplicationContext();
        AutowireCapableBeanFactory beanFactory = null;
        if (applicationContext != null) {
            beanFactory = applicationContext.getAutowireCapableBeanFactory();
        }
        try {
            Object task = entry.getObject(space);
            if (task instanceof InternalSpaceTaskWrapper) {
                task = ((InternalSpaceTaskWrapper) task).getTask();
            }

            if (task instanceof ExecutorRemotingTask)
                ((ExecutorRemotingTask) task).setServiceExporter(getServiceExporter(applicationContext));
            // go over the task and inject what can be injected
            // break when there is no more DelegatingTasks
            while (true) {
                if (task instanceof TaskGigaSpaceAware) {
                    ((TaskGigaSpaceAware) task).setGigaSpace(gigaSpace);
                } else {
                    Object field = tasksGigaSpaceInjectionMap.get(task.getClass());
                    if (field == NO_FIELD) {
                        // do nothing
                    } else if (field != null) {
                        try {
                            ((IField) field).set(task, gigaSpace);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException("Failed to set task GigaSpace field", e);
                        }
                    } else {
                        final AtomicReference<Field> ref = new AtomicReference<Field>();
                        ReflectionUtils.doWithFields(task.getClass(), new ReflectionUtils.FieldCallback() {
                            public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
                                if (field.isAnnotationPresent(TaskGigaSpace.class)) {
                                    ref.set(field);
                                }
                            }
                        });
                        if (ref.get() == null) {
                            tasksGigaSpaceInjectionMap.put(task.getClass(), NO_FIELD);
                        } else {
                            ref.get().setAccessible(true);
                            IField fastField = ReflectionUtil.createField(ref.get());
                            tasksGigaSpaceInjectionMap.put(task.getClass(), fastField);
                            try {
                                fastField.set(task, gigaSpace);
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException("Failed to set task GigaSpace field", e);
                            }
                        }
                    }
                }

                if (isAutowire(task)) {
                    if (beanFactory == null) {
                        throw new IllegalStateException("Task [" + task.getClass().getName() + "] is configured to do autowiring but the space was not started with application context");
                    }
                    beanFactory.autowireBeanProperties(task, AutowireCapableBeanFactory.AUTOWIRE_NO, false);
                    beanFactory.initializeBean(task, task.getClass().getName());
                    if (task instanceof ProcessObjectsProvider) {
                        Object[] objects = ((ProcessObjectsProvider) task).getObjectsToProcess();
                        if (objects != null) {
                            for (Object obj : objects) {
                                if (obj != null) {
                                    beanFactory.autowireBeanProperties(obj, AutowireCapableBeanFactory.AUTOWIRE_NO, false);
                                    beanFactory.initializeBean(obj, obj.getClass().getName());
                                }
                            }
                        }
                    }
                } else {
                    if (applicationContext != null && task instanceof ApplicationContextAware) {
                        ((ApplicationContextAware) task).setApplicationContext(applicationContext);
                    }
                    if (clusterInfo != null && task instanceof ClusterInfoAware) {
                        ((ClusterInfoAware) task).setClusterInfo(clusterInfo);
                    }
                }

                if (task instanceof DelegatingTask) {
                    task = ((DelegatingTask) task).getDelegatedTask();
                } else {
                    break;
                }
            }
        } catch (UnusableEntryException e) {
            // won't happen
        }
    }

    public void process(SpaceContext context, ISpaceFilterEntry[] entries, int operationCode) throws RuntimeException {

    }

    public void close() throws RuntimeException {

    }

    private boolean isAutowire(Object obj) {
        if (obj instanceof AutowireTaskMarker) {
            return true;
        }
        return obj.getClass().isAnnotationPresent(AutowireTask.class);
    }


    private SpaceRemotingServiceExporter getServiceExporter(ApplicationContext applicationContext) {
        if (serviceExporter != null)
            return serviceExporter;
        synchronized (lock) {
            if (serviceExporter == null)
                serviceExporter = ExecutorRemotingTask.getServiceExporter(applicationContext);
            return serviceExporter;
        }
    }
}
